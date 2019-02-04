package alchemist

import java.net.Socket
import java.util.{Arrays, Collections}

class WorkerSession(val address: String, val port: Int) {


  var ID: Short = _

  var sock: Socket = _

  val outputMessage = new Message
  val inputMessage = new Message

  sock = new Socket(address, port)

  def sendMessage: this.type = {

    val ar = outputMessage.finish()
    Collections.reverse(Arrays.asList(ar))
    outputMessage.print

    sock.getOutputStream.write(ar)
    sock.getOutputStream.flush

    receiveMessage
  }

  def receiveMessage: this.type = {

    val in = sock.getInputStream

    val header: Array[Byte] = Array.fill[Byte](5)(0)
    val packet: Array[Byte] = Array.fill[Byte](8192)(0)

    in.read(header, 0, 5)

    inputMessage.reset
    inputMessage.addHeader(header)

    var remainingBodyLength: Int = inputMessage.readBodyLength

    while (remainingBodyLength > 0) {
      val length: Int = Array(remainingBodyLength, 8192).min
      in.read(packet, 0, length)
      //      for (i <- 0 until length)
      //        System.out.println(s"Datatype (length):    ${packet(i)}")
      remainingBodyLength -= length
      inputMessage.addPacket(packet, length)
    }

    this
  }
}