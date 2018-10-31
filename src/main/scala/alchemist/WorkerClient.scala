package alchemist

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.DenseVector

import scala.collection.JavaConverters._
import scala.util.Random
import java.io.{FileOutputStream, InputStream, OutputStream, PrintWriter, DataInputStream => JDataInputStream, DataOutputStream => JDataOutputStream}
import java.net.Socket
import java.nio.{ByteBuffer, DoubleBuffer}

import scala.io.Source
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets
import java.util.{Arrays, Collections}

import scala.compat.Platform.EOL
import alchemist._
import alchemist.io._


//class WorkerId(val id: Int)  { }

//class WorkerInfo(val id: Short, val hostname: String, val address: String, val port: Short) {
//
//  def connect(): WorkerClient = {
//    println(s"Connecting to $hostname at $address:$port")
//    new WorkerClient(hostname, address, port)
//  }
//}

// Connects to an Alchemist worker
class WorkerClient(val ID: Short, val hostname: String, val address: String, val port: Short) {

  var sock: Socket = _
  var in: InputStream = _

  val writeMessage = new Message
  val readMessage = new Message

  var clientID: Short = 0
  var sessionID: Short = 0

//  val sock = java.nio.channels.SocketChannel.open(new java.net.InetSocketAddress(address, port))
//  println(s"Connected to $hostname at $address:$port")
//  var outbuf: ByteBuffer = null
//  var inbuf: ByteBuffer = null

  def connect(): Boolean = {
    println(s"Connecting to Alchemist at $address:$port")

    sock = new Socket(address, port)

    in = sock.getInputStream

    handshake
  }

  def sendMessage: this.type = {

    val ar = writeMessage.finish()
    Collections.reverse(Arrays.asList(ar))

//    writeMessage.print

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

    var remainingBodyLength: Int = readMessage.readBodyLength()

    while (remainingBodyLength > 0) {
      val length: Int = Array(remainingBodyLength, 8192).min
      in.read(packet, 0, length)
      //      for (i <- 0 until length)
      //        System.out.println(s"Datatype (length):    ${packet(i)}")
      remainingBodyLength -= length
      readMessage.addPacket(packet, length)
    }

//    readMessage.print

    this
  }

  def handshake: Boolean = {

    writeMessage.start(0, 0, "HANDSHAKE")

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

  def startSendMatrixBlocks(id: Short): this.type = {
    writeMessage.start(clientID, sessionID, "SEND_MATRIX_BLOCKS")
    writeMessage.writeShort(id)

    this
  }

  def addSendMatrixBlock(blockRange: Array[Long], block: Array[Double]): this.type = {

    blockRange.foreach(i => writeMessage.writeLong(i))
    block.foreach(v => writeMessage.writeDouble(v))

    this
  }

  def finishSendMatrixBlocks: this.type = {
    sendMessage

    this
  }

  def startRequestMatrixBlocks(id: Short): this.type = {
    writeMessage.start(clientID, sessionID, "REQUEST_MATRIX_BLOCKS")
    writeMessage.writeShort(id)

    this
  }

  def addRequestedMatrixBlock(blockRange: Array[Long]): this.type = {

    blockRange.foreach(i => writeMessage.writeLong(i))

    this
  }

  def getRequestedMatrixBlock(blockRange: Array[Long]): DenseVector = {

    if (readMessage.readPos == readMessage.headerLength)
      readMessage.readShort()

    val rowStart = readMessage.readLong()
    val rowEnd = readMessage.readLong()
    val colStart = readMessage.readLong()
    val colEnd = readMessage.readLong()

    val numCols = colEnd - colStart + 1

    new DenseVector(readMessage.readDouble(numCols.toInt))
  }

  def finishRequestMatrixBlocks: this.type = {
    sendMessage

    this
  }

//  private def sendMessage(outbuf: ByteBuffer): Unit = {
//    assert(!outbuf.hasRemaining())
//    outbuf.rewind()
//    while(sock.write(outbuf) != 0) { }
//    outbuf.clear()
//    assert(outbuf.position() == 0)
//  }
//
//  private def beginOutput(length: Int): ByteBuffer = {
//    if(outbuf == null) {
//      outbuf = ByteBuffer.allocate(Math.max(length, 16 * 1024 * 1024))
//    }
//    assert(outbuf.position() == 0)
//    if (outbuf.capacity() < length) {
//      outbuf = ByteBuffer.allocate(length)
//    }
//    outbuf.limit(length)
//    return outbuf
//  }
//
//  private def beginInput(length: Int): ByteBuffer = {
//    if(inbuf == null || inbuf.capacity() < length) {
//      inbuf = ByteBuffer.allocate(Math.max(length, 16 * 1024 * 1024))
//    }
//    inbuf.clear().limit(length)
//    return inbuf
//  }
//
//  def newMatrixAddRow(handle: MatrixHandle, rowIdx: Long, vals: Array[Double]) = {
//    val outbuf = beginOutput(4 + 4 + 8 + 8 + 8 * vals.length)
//    outbuf.putInt(0x1)  // typeCode = addRow
//    outbuf.putInt(handle.id)
//    outbuf.putLong(rowIdx)
//    outbuf.putLong(vals.length * 8)
//    outbuf.asDoubleBuffer().put(vals)
//    outbuf.position(outbuf.position() + 8 * vals.length)
//    //System.err.println(s"Sending row ${rowIdx} to ${hostname}:${port}")
//    sendMessage(outbuf)
//    //System.err.println(s"Sent row ${rowIdx} successfully")
//  }
//
//  def newMatrixPartitionComplete(handle: MatrixHandle) = {
//    val outbuf = beginOutput(4)
//    outbuf.putInt(0x2)  // typeCode = partitionComplete
//    sendMessage(outbuf)
//  }
//
//  def getIndexedRowMatrixRow(handle: MatrixHandle, rowIndex: Long, numCols: Int) : DenseVector = {
//    val outbuf = beginOutput(4 + 4 + 8)
//    outbuf.putInt(0x3) // typeCode = getRow
//    outbuf.putInt(handle.id)
//    outbuf.putLong(rowIndex)
//    sendMessage(outbuf)
//
//    val inbuf = beginInput(8 + 8 * numCols)
//    while(inbuf.hasRemaining()) {
//      sock.read(inbuf)
//    }
//    inbuf.flip()
//    assert(numCols * 8 == inbuf.getLong())
//    val vec = new Array[Double](numCols)
//    inbuf.asDoubleBuffer().get(vec)
//    return new DenseVector(vec)
//  }
//
//  def getIndexedRowMatrixPartitionComplete(handle: MatrixHandle) = {
//    val outbuf = beginOutput(4)
//    println(s"Finished getting rows on worker")
//    outbuf.putInt(0x4) // typeCode = doneGettingRows
//    sendMessage(outbuf)
//  }

  def disconnectFromAlchemist: this.type = {
    println(s"Disconnecting from Alchemist")
    sock.close()
    this
  }

  def close(): this.type = {
    disconnectFromAlchemist
  }
}