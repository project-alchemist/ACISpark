package alchemist

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.DenseVector
import scala.collection.JavaConverters._
import scala.util.Random
import java.io.{
    PrintWriter, FileOutputStream,
    InputStream, OutputStream,
    DataInputStream => JDataInputStream,
    DataOutputStream => JDataOutputStream
}
import java.nio.{
    DoubleBuffer, ByteBuffer
}
import scala.io.Source
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets
import scala.compat.Platform.EOL

import alchemist._
import alchemist.io._


class WorkerId(val id: Int)  { }

class WorkerInfo(val id: Short, val hostname: String, val address: String, val port: Short) {

  def connect(): WorkerClient = {
    println(s"Connecting to $hostname at $address:$port")
    new WorkerClient(hostname, address, port)
  }
}

class WorkerClient(val hostname: String, val address: String, val port: Int) {
  
  val sock = java.nio.channels.SocketChannel.open(new java.net.InetSocketAddress(address, port))
  println(s"Connected to $hostname at $address:$port")
  var outbuf: ByteBuffer = null
  var inbuf: ByteBuffer = null

  private def sendMessage(outbuf: ByteBuffer): Unit = {
    assert(!outbuf.hasRemaining())
    outbuf.rewind()
    while(sock.write(outbuf) != 0) { }
    outbuf.clear()
    assert(outbuf.position() == 0)
  }

  private def beginOutput(length: Int): ByteBuffer = {
    if(outbuf == null) {
      outbuf = ByteBuffer.allocate(Math.max(length, 16 * 1024 * 1024))
    }
    assert(outbuf.position() == 0)
    if (outbuf.capacity() < length) {
      outbuf = ByteBuffer.allocate(length)
    }
    outbuf.limit(length)
    return outbuf
  }

  private def beginInput(length: Int): ByteBuffer = {
    if(inbuf == null || inbuf.capacity() < length) {
      inbuf = ByteBuffer.allocate(Math.max(length, 16 * 1024 * 1024))
    }
    inbuf.clear().limit(length)
    return inbuf
  }

  def newMatrixAddRow(handle: MatrixHandle, rowIdx: Long, vals: Array[Double]) = {
    val outbuf = beginOutput(4 + 4 + 8 + 8 + 8 * vals.length)
    outbuf.putInt(0x1)  // typeCode = addRow
    outbuf.putInt(handle.id)
    outbuf.putLong(rowIdx)
    outbuf.putLong(vals.length * 8)
    outbuf.asDoubleBuffer().put(vals)
    outbuf.position(outbuf.position() + 8 * vals.length)
    //System.err.println(s"Sending row ${rowIdx} to ${hostname}:${port}")
    sendMessage(outbuf)
    //System.err.println(s"Sent row ${rowIdx} successfully")
  }

  def newMatrixPartitionComplete(handle: MatrixHandle) = {
    val outbuf = beginOutput(4)
    outbuf.putInt(0x2)  // typeCode = partitionComplete
    sendMessage(outbuf)
  }

  def getIndexedRowMatrixRow(handle: MatrixHandle, rowIndex: Long, numCols: Int) : DenseVector = {
    val outbuf = beginOutput(4 + 4 + 8)
    outbuf.putInt(0x3) // typeCode = getRow
    outbuf.putInt(handle.id)
    outbuf.putLong(rowIndex)
    sendMessage(outbuf)

    val inbuf = beginInput(8 + 8 * numCols)
    while(inbuf.hasRemaining()) {
      sock.read(inbuf)
    }
    inbuf.flip()
    assert(numCols * 8 == inbuf.getLong())
    val vec = new Array[Double](numCols)
    inbuf.asDoubleBuffer().get(vec)
    return new DenseVector(vec)
  }

  def getIndexedRowMatrixPartitionComplete(handle: MatrixHandle) = {
    val outbuf = beginOutput(4)
    println(s"Finished getting rows on worker")
    outbuf.putInt(0x4) // typeCode = doneGettingRows
    sendMessage(outbuf)
  }

  def close() = {
    sock.close()
  }
}