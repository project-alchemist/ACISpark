package alchemist

import java.io.InputStream
import java.net.Socket
import java.util.{Arrays, Collections}

@SerialVersionUID(12L)
class Client extends Serializable {

  var hostname: String = _
  var address: String = _
  var port: Int = _

  var ID: Short = _

  //  var driverProc: Process = _

  var clientID: Short = 0
  var sessionID: Short = 0

  var sock: Socket = _
  var in: InputStream = _

  var writeMessage = new Message
  var readMessage = new Message


  def connect(idx: Int = -1): Boolean = {

    try {
      if (idx < 0)
        println(s"Connecting to Alchemist at $hostname:$port")
      else
        println(s"Spark executor ${idx}: Connecting to Alchemist at $hostname:$port")

      sock = new Socket(address, port)

      handshake._1
    }
    catch {
      case e: Exception => {
        println("Alchemist appears to be offline")
        println("Returned error: " + e)

        false
      }
    }
  }

  def connect(_hostname: String, _port: Int): Boolean = {

    hostname = _hostname
    port = _port

    connect(-1)
  }

  def setBufferLength(bufferLength: Int = 10000000): this.type = {

    writeMessage.setBufferLength(bufferLength)
    readMessage.setBufferLength(bufferLength)

    this
  }

  def sendMessage: (Long, Long) = {

    val startTime = System.nanoTime
    val ar = writeMessage.finish
    Collections.reverse(Arrays.asList(ar))

//    writeMessage.print

    sock.getOutputStream.write(ar)
    sock.getOutputStream.flush

    (writeMessage.headerLength + writeMessage.bodyLength, System.nanoTime - startTime)
  }

  def receiveMessage: (Long, Long) = {

    val in = sock.getInputStream

    val header: Array[Byte] = Array.fill[Byte](readMessage.headerLength)(0)
    val packet: Array[Byte] = Array.fill[Byte](8192)(0)

    in.read(header, 0, readMessage.headerLength)

    val startTime = System.nanoTime

    readMessage.reset
      .addHeader(header)

    var remainingBodyLength: Int = readMessage.bodyLength

    while (remainingBodyLength > 0) {
      val length: Int = Array(remainingBodyLength, 8192).min
      in.read(packet, 0, length)
      remainingBodyLength -= length
      readMessage.addPacket(packet, length)
    }

    readMessage.readHeader.resetPosition
//    readMessage.print

    (readMessage.headerLength + readMessage.bodyLength, System.nanoTime - startTime)
  }

  def handshake: (Boolean, Overhead, Overhead) = {

    val testMatrix: MatrixBlock = new MatrixBlock(
      (for {r <- 3 to 14} yield 1.11*r).toArray, Array[Long](0l, 3l, 1l), Array[Long](0l, 2l, 1l)
    )

    val sendStartTime = System.nanoTime

    writeMessage.start(0, 0, Command.Handshake)
      .writeByte(2)
      .writeShort(1234)
      .writeString("ABCD")
      .writeDouble(1.11)
      .writeDouble(2.22)
      .writeMatrixBlock(testMatrix)
      .writeInt(writeMessage.maxBodyLength + writeMessage.headerLength)

    val (sendBytes, sendTime) = sendMessage
    val sendOverhead = new Overhead(0, sendBytes, sendTime, System.nanoTime - sendStartTime)

    val receiveStartTime = System.nanoTime
    val (receiveBytes, receiveTime) = receiveMessage

    val validHandshake: Boolean = {
      if (readMessage.readCommandCode == 1 && readMessage.readShort == 4321 &&
        readMessage.readString == "DCBA" && readMessage.readDouble ==  3.33) {
          clientID = readMessage.readClientID
          sessionID = readMessage.readSessionID

          true
        }
      else false
    }

    val receiveOverhead = new Overhead(1, receiveBytes, receiveTime, System.nanoTime - receiveStartTime)

    (validHandshake, sendOverhead, receiveOverhead)
  }

  def requestClientID: (Overhead, Overhead) = {

    val sendStartTime = System.nanoTime

    writeMessage.start(clientID, sessionID, Command.RequestId)

    val (sendBytes, sendTime) = sendMessage
    val sendOverhead = new Overhead(0, sendBytes, sendTime, System.nanoTime - sendStartTime)

    val receiveStartTime = System.nanoTime
    val (receiveBytes, receiveTime) = receiveMessage

    ID = readMessage.readClientID

    val receiveOverhead = new Overhead(1, receiveBytes, receiveTime, System.nanoTime - receiveStartTime)

    (sendOverhead, receiveOverhead)
  }

  def clientInfo(numWorkers: Byte, logDir: String): (Overhead, Overhead) = {

    val sendStartTime = System.nanoTime

    writeMessage.start(clientID, sessionID, Command.ClientInfo)
      .writeShort(numWorkers)
      .writeString(logDir)

    val (sendBytes, sendTime) = sendMessage
    val sendOverhead = new Overhead(0, sendBytes, sendTime, System.nanoTime - sendStartTime)

    val receiveStartTime = System.nanoTime
    val (receiveBytes, receiveTime) = receiveMessage

    val receiveOverhead = new Overhead(1, receiveBytes, receiveTime, System.nanoTime - receiveStartTime)

    (sendOverhead, receiveOverhead)
  }

  def sendTestString(testString: String): (String, Overhead, Overhead) = {

    val sendStartTime = System.nanoTime

    writeMessage.start(clientID, sessionID, Command.SendTestString)
      .writeString(testString)

    val (sendBytes, sendTime) = sendMessage
    val sendOverhead = new Overhead(0, sendBytes, sendTime, System.nanoTime - sendStartTime)

    val receiveStartTime = System.nanoTime
    val (receiveBytes, receiveTime) = receiveMessage

    val responseString: String = readMessage.readString

    val receiveOverhead = new Overhead(1, receiveBytes, receiveTime, System.nanoTime - receiveStartTime)

    (responseString, sendOverhead, receiveOverhead)
  }

  def requestTestString: (String, Overhead, Overhead) = {

    val sendStartTime = System.nanoTime

    writeMessage.start(clientID, sessionID, Command.RequestTestString)

    val (sendBytes, sendTime) = sendMessage
    val sendOverhead = new Overhead(0, sendBytes, sendTime, System.nanoTime - sendStartTime)

    val receiveStartTime = System.nanoTime
    val (receiveBytes, receiveTime) = receiveMessage

    val testString: String = readMessage.readString

    val receiveOverhead = new Overhead(1, receiveBytes, receiveTime, System.nanoTime - receiveStartTime)

    (testString, sendOverhead, receiveOverhead)
  }

  def close: (Overhead, Overhead) = disconnectFromAlchemist

  def disconnectFromAlchemist: (Overhead, Overhead) = {

    val sendStartTime = System.nanoTime

    writeMessage.start(clientID, sessionID, Command.CloseConnection)

    val (sendBytes, sendTime) = sendMessage
    val sendOverhead = new Overhead(0, sendBytes, sendTime, System.nanoTime - sendStartTime)

    val receiveStartTime = System.nanoTime
    val (receiveBytes, receiveTime) = receiveMessage

    val receiveOverhead = new Overhead(1, receiveBytes, receiveTime, System.nanoTime - receiveStartTime)

    (sendOverhead, receiveOverhead)
  }

}
