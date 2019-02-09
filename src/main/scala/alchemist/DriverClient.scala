package alchemist

import scala.collection.JavaConverters._
import scala.collection.Map
import scala.util.Random

import java.net.Socket
import java.nio.{ByteBuffer, ByteOrder}
import java.util.{Collections, Arrays}
import java.io.{BufferedReader, FileInputStream, InputStream, InputStreamReader, OutputStream, PrintWriter, DataInputStream => JDataInputStream, DataOutputStream => JDataOutputStream}

import scala.io.Source
import org.apache.spark.sql.SparkSession
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets

import scala.compat.Platform.EOL


class DriverClient {          // Connects to the Alchemist driver
  
  var address: String = _
  var port: Int = _

  var ID: Short = _

  var driverProc: Process = _

  var clientID: Short = 0
  var sessionID: Short = 0

  var sock: Socket = _
  var in: InputStream = _

  val writeMessage = new Message
  val readMessage = new Message

//  var workerInfo: Array[WorkerInfo] = Array.empty[WorkerInfo]

    
//  val driverSock = listenSock.accept()
//  System.err.println(s"Alchemist.Driver: Accepting connection from Alchemist driver on socket")
  var client: DriverSession = _

  def connect(address: String, port: Int): Boolean = {

    val pb = new ProcessBuilder("true")

    driverProc = pb.redirectError(ProcessBuilder.Redirect.INHERIT)
      .redirectOutput(ProcessBuilder.Redirect.INHERIT)
      .start

    try {
      sock = new Socket(address, port)

      client = new DriverSession(sock.getInputStream, sock.getOutputStream)

      handshake
    }
    catch {
      case e: Exception => {
        println("Alchemist appears to be offline")
        println("Returned error: " + e)

        false
      }
    }

  }

  def sendMessage: this.type = {

    val ar = writeMessage.finish()
    Collections.reverse(Arrays.asList(ar))

    writeMessage.print

    sock.getOutputStream.write(ar)
    sock.getOutputStream.flush

    receiveMessage
  }

  def receiveMessage: this.type = {

    val in = sock.getInputStream

    val header: Array[Byte] = Array.fill[Byte](9)(0)
    val packet: Array[Byte] = Array.fill[Byte](8192)(0)

    in.read(header, 0, 9)

    readMessage.reset
    readMessage.addHeader(header)

    var remainingBodyLength: Int = readMessage.readBodyLength

    while (remainingBodyLength > 0) {
      val length: Int = Array(remainingBodyLength, 8192).min
      in.read(packet, 0, length)
//      for (i <- 0 until length)
//        System.out.println(s"Datatype (length):    ${packet(i)}")
      remainingBodyLength -= length
      readMessage.addPacket(packet, length)
    }

    readMessage.print

    this
  }


//  def handleMessage: this.type = {
//
//    val cc = readMessage.readCommandCode
//
//    cc match {
//      case  0 => wait
//      case  2 => requestID
//      case  3 => clientInfo
//      case  4 => sendTestString
//      case  5 => requestTestString
//      case  6 => requestWorkers
//      case  7 => yieldWorkers
//      case  8 => sendAssignedWorkersInfo
//      case  9 => listAllWorkers
//      case 10 => listActiveWorkers
//      case 11 => listInactiveWorkers
//      case 12 => listAssignedWorkers
//      case 13 => loadLibrary
//      case 14 => runTask
//      case 15 => unloadLibrary
//      case 16 => matrixInfo
//      case 17 => matrixLayout
//      case 18 => matrixBlock
//    }
//
//    this
//  }

  def handshake: Boolean = {

    writeMessage.start(0, 0, Command.Handshake)

    writeMessage.writeByte(2)
    writeMessage.writeShort(1234)
    writeMessage.writeString("ABCD")

    sendMessage

    var handshakeSuccess: Boolean = false

    if (readMessage.readCommandCode == 1) {
      if (readMessage.readShort == 4321) {
        if (readMessage.readString == "DCBA") {
          clientID = readMessage.readClientID
          sessionID = readMessage.readSessionID
        }
      }
    }

    handshakeSuccess
  }

  def requestID: this.type = {

    writeMessage.start(clientID, sessionID, Command.RequestId)

    sendMessage

    if (readMessage.readCommandCode == 2) {
      ID = readMessage.readShort
    }

    this
  }

  def clientInfo(numWorkers: Byte, logDir: String): this.type = {

    writeMessage.start(clientID, sessionID, Command.ClientInfo)
    writeMessage.writeShort(numWorkers)
    writeMessage.writeString(logDir)

    sendMessage

    if (readMessage.readCommandCode == 3) {
//      ID = readMessage.readShort
    }

    this
  }

  def sendTestString(testString: String): String = {

    writeMessage.start(clientID, sessionID, Command.SendTestString)
    writeMessage.writeString(testString)

    sendMessage

    var responseString: String = ""

    if (readMessage.readCommandCode == 4) {
      responseString = readMessage.readString
    }

    responseString
  }

  def requestTestString: String = {

    writeMessage.start(clientID, sessionID, Command.RequestTestString)

    sendMessage

    var testString: String = ""

    if (readMessage.readCommandCode == 4) {
      testString = readMessage.readString
    }

    testString
  }

  def requestWorkers(numWorkers: Byte): Map[Byte, WorkerClient] = {

    println(s"Requesting $numWorkers Alchemist workers")

    writeMessage.start(clientID, sessionID, Command.RequestWorkers)
    writeMessage.writeShort(numWorkers)

    sendMessage

    val numAssignedWorkers: Byte = readMessage.readByte()


//    var workerClients: Array[WorkerClient] = Array.empty[WorkerClient]
//
//    if (numAssignedWorkers > 0) {
//      workerClients =
//    }
//    else {
//
//    }

    var workers: Map[Byte, WorkerClient] = Map.empty[Byte, WorkerClient]

    (0 until numAssignedWorkers).foreach(_ => {
      val ID: Byte = readMessage.readByte
      workers += (ID -> new WorkerClient(ID, readMessage.readString, readMessage.readString, readMessage.readByte))
    })

    workers
  }

  def yieldWorkers(yieldedWorkers: List[Byte] = List.empty[Byte]): List[Byte] = {

    println(s"Yielding Alchemist workers")

    writeMessage.start(clientID, sessionID, Command.YieldWorkers)

    sendMessage

    val message: String = readMessage.readString

    println(message)

    yieldedWorkers
  }

  def sendArrayInfo(numRows: Long, numCols: Long): ArrayHandle = {

    writeMessage.start(clientID, sessionID, Command.ArrayInfo)
    writeMessage.writeByte(0)        // Type: dense
    writeMessage.writeByte(0)        // Layout: by rows (default)
    writeMessage.writeLong(numRows)         // Number of rows
    writeMessage.writeLong(numCols)         // Number of columns

    sendMessage

    val matrixID: Short = readMessage.readShort
    val rowLayout: Array[Short] = extractLayout

    new ArrayHandle(matrixID)
  }

  def extractLayout: Array[Short] = {

    val numRows: Long = readMessage.readLong

    (0l until numRows).map(_ => readMessage.readShort).toArray
  }

  def sendAssignedWorkersInfo(preamble: String): this.type = {

    writeMessage.start(clientID, sessionID, Command.SendAssignedWorkersInfo)

    sendMessage
  }

  def listAllWorkers(preamble: String): this.type = {

    writeMessage.start(clientID, sessionID, Command.ListAllWorkers)

    sendMessage
  }

  def listActiveWorkers(preamble: String): this.type = {

    writeMessage.start(clientID, sessionID, Command.ListActiveWorkers)

    sendMessage
  }

  def listInactiveWorkers(preamble: String): this.type = {

    writeMessage.start(clientID, sessionID, Command.ListInactiveWorkers)

    sendMessage
  }

  def listAssignedWorkers(preamble: String): this.type = {

    writeMessage.start(clientID, sessionID, Command.ListAssignedWorkers)

    sendMessage
  }

  def loadLibrary(preamble: String): this.type = {

    writeMessage.start(clientID, sessionID, Command.LoadLibrary)

    sendMessage
  }

  def runTask(preamble: String): this.type = {

    writeMessage.start(clientID, sessionID, Command.RunTask)

    sendMessage
  }

  def unloadLibrary(preamble: String): this.type = {

    writeMessage.start(clientID, sessionID, Command.UnloadLibrary)

    sendMessage
  }

  def matrixInfo: this.type = {

    writeMessage.start(clientID, sessionID, Command.ArrayInfo)

    sendMessage
  }

  def matrixLayout: this.type = {

    writeMessage.start(clientID, sessionID, Command.ArrayLayout)

    sendMessage
  }

  def matrixBlock: this.type = {

    writeMessage.start(clientID, sessionID, Command.RequestArrayBlocks) // TODO: Not sure which one this is.

    sendMessage
  }

  def disconnectFromAlchemist: this.type = {
    println(s"Disconnecting from Alchemist")
    sock.close()
    this
  }

  def close: this.type = {
    yieldWorkers()
    disconnectFromAlchemist
  }
}
