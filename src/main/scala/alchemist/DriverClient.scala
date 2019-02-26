package alchemist

import scala.collection.JavaConverters._
import scala.collection.Map
import scala.reflect.{ClassTag, classTag}
import scala.util.Random
import java.net.Socket
import java.nio.{ByteBuffer, ByteOrder}
import java.util.{Arrays, Collections}
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

//  var driverProc: Process = _

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

//    val pb = new ProcessBuilder("true")
//
//    driverProc = pb.redirectError(ProcessBuilder.Redirect.INHERIT)
//      .redirectOutput(ProcessBuilder.Redirect.INHERIT)
//      .start

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

    val header: Array[Byte] = Array.fill[Byte](readMessage.headerLength)(0)
    val packet: Array[Byte] = Array.fill[Byte](8192)(0)

    in.read(header, 0, readMessage.headerLength)

    readMessage.reset
               .addHeader(header)

    var remainingBodyLength: Int = readMessage.bodyLength

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

  def runTask(lib: LibraryHandle, methodName: String, inArgs: Parameters): Parameters = {

    writeMessage.start(clientID, sessionID, Command.RunTask)
    writeMessage.writeLibraryID(lib.id)
    writeMessage.writeString(methodName)
    inArgs.getParameters.foreach { case (name, value) => serializeParameter(name, value) }

    sendMessage

    deserializeParameters
  }

  def deserializeParameters: Parameters = {
    val outArgs: Parameters = new Parameters
    while (!readMessage.eom) {
      var p = readMessage.readParameter

      p match {
        case Parameter(n: String, v: Byte) => outArgs.add(p.asInstanceOf[Parameter[Byte]])
        case Parameter(n: String, v: Short) => outArgs.add(p.asInstanceOf[Parameter[Short]])
        case Parameter(n: String, v: Int) => outArgs.add(p.asInstanceOf[Parameter[Int]])
        case Parameter(n: String, v: Long) => outArgs.add(p.asInstanceOf[Parameter[Long]])
        case Parameter(n: String, v: Float) => outArgs.add(p.asInstanceOf[Parameter[Float]])
        case Parameter(n: String, v: Double) => outArgs.add(p.asInstanceOf[Parameter[Double]])
        case Parameter(n: String, v: Char) => outArgs.add(p.asInstanceOf[Parameter[Char]])
        case Parameter(n: String, v: String) => outArgs.add(p.asInstanceOf[Parameter[String]])
        case Parameter(n: String, v: ArrayID) => outArgs.add(p.asInstanceOf[Parameter[ArrayID]])
        case _ => "UNKNOWN TYPE"
      }
    }

    outArgs.add[Int]("rank", 32)

    outArgs
  }

  def serializeParameter(name: String, p: ParameterValue): Unit = {

    writeMessage.writeParameter.writeString(name)
    p match {
      case Parameter(n: String, v: Byte) => writeMessage.writeByte(p.asInstanceOf[Parameter[Byte]].value)
      case Parameter(n: String, v: Short) => writeMessage.writeShort(p.asInstanceOf[Parameter[Short]].value)
      case Parameter(n: String, v: Int) => writeMessage.writeInt(p.asInstanceOf[Parameter[Int]].value)
      case Parameter(n: String, v: Long) => writeMessage.writeLong(p.asInstanceOf[Parameter[Long]].value)
      case Parameter(n: String, v: Float) => writeMessage.writeFloat(p.asInstanceOf[Parameter[Float]].value)
      case Parameter(n: String, v: Double) => writeMessage.writeDouble(p.asInstanceOf[Parameter[Double]].value)
      case Parameter(n: String, v: Char) => writeMessage.writeChar(p.asInstanceOf[Parameter[Char]].value)
      case Parameter(n: String, v: String) => writeMessage.writeString(p.asInstanceOf[Parameter[String]].value)
      case Parameter(n: String, v: ArrayID) => writeMessage.writeArrayID(p.asInstanceOf[Parameter[ArrayID]].value)
      case _ => println("Unknown type")
    }
  }

//  def deserializeParameter: Parameter = {
//
//    writeMessage.writeParameter.writeString(p.name)
//    p.datatype.value match {
//      case Datatype.Byte.value => writeMessage.writeByte(p.value)
//      case Datatype.Short.value => writeMessage.writeShort(p.value)
//      case Datatype.Int.value => writeMessage.writeInt(p.value)
//      case Datatype.Long.value => writeMessage.writeLong(p.value)
//      case Datatype.Float.value => writeMessage.writeFloat(p.value)
//      case Datatype.Double.value => writeMessage.writeDouble(p.value)
//      case Datatype.Char.value => writeMessage.writeChar(p.value)
//      case Datatype.String.value => writeMessage.writeString(p.value)
//      case Datatype.ArrayID.value => writeMessage.writeArrayID(p.value)
//    }
//  }

  def handshake: Boolean = {

    val testArray: ArrayBlockDouble = new ArrayBlockDouble(
      Array[Long](0l, 3l, 1l, 0l, 2l, 1l).grouped(3).toArray,
      (for {r <- 3 to 14} yield 1.11*r).toArray
    )

    writeMessage.start(0, 0, Command.Handshake)
                .writeByte(2)
                .writeShort(1234)
                .writeString("ABCD")
                .writeDouble(1.11)
                .writeDouble(2.22)
                .writeArrayBlockDouble(testArray)

    sendMessage

    validateHandshake
  }

  def validateHandshake: Boolean = {

    if (readMessage.readCommandCode == 1 && readMessage.readShort == 4321 &&
        readMessage.readString == "DCBA" && readMessage.readDouble ==  3.33) {
      clientID = readMessage.readClientID
      sessionID = readMessage.readSessionID

      return true
    }

    false
  }

  def requestClientID: this.type = {

    writeMessage.start(clientID, sessionID, Command.RequestId)

    sendMessage

    ID = readMessage.readClientID

    this
  }

  def clientInfo(numWorkers: Byte, logDir: String): this.type = {

    writeMessage.start(clientID, sessionID, Command.ClientInfo)
                .writeShort(numWorkers)
                .writeString(logDir)

    sendMessage

    this
  }

  def sendTestString(testString: String): String = {

    writeMessage.start(clientID, sessionID, Command.SendTestString)
                .writeString(testString)

    sendMessage

    readMessage.readString
  }

  def requestTestString: String = {

    writeMessage.start(clientID, sessionID, Command.RequestTestString)

    sendMessage

    readMessage.readString
  }

  def requestWorkers(numWorkers: Short): Map[Short, WorkerClient] = {


    writeMessage.start(clientID, sessionID, Command.RequestWorkers)
                .writeShort(numWorkers)

    sendMessage

    val numAssignedWorkers: Short = readMessage.readShort

    var workers: Map[Short, WorkerClient] = Map.empty[Short, WorkerClient]

    0 until numAssignedWorkers foreach(_ => {
      val w: WorkerInfo = readMessage.readWorkerInfo
      workers += (w.ID -> new WorkerClient(w.ID, w.hostname, w.address, w.port))
    })

    workers
  }

  def yieldWorkers(yieldedWorkers: List[Byte] = List.empty[Byte]): List[Byte] = {

    println(s"Yielding Alchemist workers")

    writeMessage.start(clientID, sessionID, Command.YieldWorkers)

    sendMessage

    val message: String = readMessage.readString

    yieldedWorkers
  }

  def sendArrayInfo(numRows: Long, numCols: Long): ArrayHandle = {

    writeMessage.start(clientID, sessionID, Command.ArrayInfo)
                .writeByte(0)        // Type: dense
                .writeByte(0)        // Layout: by rows (default)
                .writeLong(numRows)         // Number of rows
                .writeLong(numCols)         // Number of columns

    sendMessage

    val arrayID: ArrayID = readMessage.readArrayID
    val rowLayout: Array[Short] = extractLayout

    new ArrayHandle(arrayID)
  }

  def extractLayout: Array[Short] = {

    val numRows: Long = readMessage.readLong

    (0l until numRows).map(_ => readMessage.readShort).toArray
  }

  def sendAssignedWorkersInfo(preamble: String): this.type = {

    writeMessage.start(clientID, sessionID, Command.SendAssignedWorkersInfo)

    sendMessage
  }

  def readWorkerList: Array[WorkerInfo] = {

    var workerList: Array[WorkerInfo] = Array.empty[WorkerInfo]

    val numWorkers = readMessage.readShort.toInt
    0 until numWorkers foreach { _ => workerList = workerList :+ readMessage.readWorkerInfo }

    workerList
  }

  def listAllWorkers: Array[WorkerInfo] = {

    writeMessage.start(clientID, sessionID, Command.ListAllWorkers)

    sendMessage

    readWorkerList
  }

  def listActiveWorkers: Array[WorkerInfo] = {

    writeMessage.start(clientID, sessionID, Command.ListActiveWorkers)

    sendMessage

    readWorkerList
  }

  def listInactiveWorkers: Array[WorkerInfo] = {

    writeMessage.start(clientID, sessionID, Command.ListInactiveWorkers)

    sendMessage

    readWorkerList
  }

  def listAssignedWorkers: Array[WorkerInfo] = {

    writeMessage.start(clientID, sessionID, Command.ListAssignedWorkers)

    sendMessage

    readWorkerList
  }

  def loadLibrary(name: String, path: String): LibraryHandle = {

    writeMessage.start(clientID, sessionID, Command.LoadLibrary)
                .writeString(name)
                .writeString(path)

    sendMessage

    LibraryHandle(readMessage.readLibraryID, name, path)
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
