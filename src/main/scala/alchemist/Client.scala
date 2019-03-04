package alchemist

import java.io.InputStream
import java.net.Socket
import java.util.{Arrays, Collections}

class Client {

  var hostname: String = _
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


  def connect: Boolean = {

    try {
      println(s"Connecting to Alchemist at $address:$port")

      sock = new Socket(address, port)

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

  def connect(_address: String, _port: Int): Boolean = {

    address = _address
    port = _port

    connect
  }

  def sendMessage: this.type = {

    val ar = writeMessage.finish()
    Collections.reverse(Arrays.asList(ar))

    writeMessage.print

    sock.getOutputStream.write(ar)
    sock.getOutputStream.flush

    println("uy 1")

    receiveMessage
    println("uy 2")

    this
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

  def disconnectFromAlchemist: this.type = {
    println(s"Disconnecting from Alchemist")
    sock.close()
    this
  }

}
