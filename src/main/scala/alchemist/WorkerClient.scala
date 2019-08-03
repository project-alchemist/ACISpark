package alchemist

import java.net.Socket
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.distributed.IndexedRow

@SerialVersionUID(14L)
class WorkerClient extends Client with Serializable {

  def connect(_ID: Short, _hostname: String, _address: String, _port: Short): Boolean = {

    ID = _ID
    hostname = _hostname
    address = _address
    port = _port

    try {
      sock = new Socket(hostname, port)

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

  def startSendIndexedRows(id: MatrixID): this.type = {
    writeMessage.start(clientID, sessionID, Command.SendIndexedRows)
    writeMessage.writeMatrixID(id)

    this
  }

  def sendIndexedRows(mh: MatrixHandle,
                      indexedRows: Array[IndexedRow],
                      idx: Int): (Array[Overhead], Array[Overhead]) = {

    val times: Array[Long] = Array.fill[Long](4)(0)

    val rows: Array[Long] = mh.getRowAssignments(ID)
    val cols: Array[Long] = mh.getColAssignments(ID)

    val numRows: Long = math.ceil((rows(1) - rows(0) + 1) / rows(2)).toLong
    val numCols: Long = math.ceil((cols(1) - cols(0) + 1) / cols(2)).toLong

    val numElements: Long = numRows * numCols
    val numMessages: Short = math.ceil((numElements * 8.0) / writeMessage.maxBodyLength).toShort

    val numRowsPerMessage: Long = math.ceil(numRows / numMessages).toLong
    var numSentElements: Long = 0l

    var rowStart: Long = 0
    var rowEnd: Long = 0

    var sendOverheads: Array[Overhead] = Array.empty[Overhead]
    var receiveOverheads: Array[Overhead] = Array.empty[Overhead]

    for (m <- 0 until numMessages) {

      val sendStartTime = System.nanoTime

      writeMessage.start(clientID, sessionID, Command.SendMatrixBlocks)
      writeMessage.writeMatrixID(mh.id)

      rowEnd += (numRowsPerMessage - 1) * rows(2)

      var localRows: Array[Long] = Array.empty[Long]

      indexedRows foreach (row => {
        localRows = localRows :+ row.index
      } )

      localRows.sorted

      rowStart = localRows(0) + rows(2) - (localRows(0) % rows(2)) - 1
      rowEnd = localRows.last

      val messageRows: Array[Long] = Array(rowStart, rowEnd, rows(2))

      val block: MatrixBlock = new MatrixBlock(Array.empty[Double], messageRows, cols)

      writeMessage.writeMatrixBlock(block)

      for (i <- rowStart to rowEnd by rows(2)) {
        indexedRows foreach (row => {
          if (row.index == i) {
            val rowArray: Array[Double] = row.vector.toArray
            for (j <- cols(0) to cols(1) by cols(2))
              writeMessage.putDouble(rowArray(j.toInt))
          }
        })
      }

      val (sendBytes, sendTime) = sendMessage
      sendOverheads = sendOverheads :+ new Overhead(0, sendBytes, sendTime, System.nanoTime - sendStartTime)

      val (receiveBytes, receiveTime) = receiveMessage
      val receiveStartTime = System.nanoTime

      receiveOverheads = receiveOverheads :+ new Overhead(1, receiveBytes, receiveTime, System.nanoTime - receiveStartTime)

      rowStart = rowEnd + rows(2)
    }

    (sendOverheads, receiveOverheads)
  }

  def getIndexedRows(mh: MatrixHandle,
                     rowIndices: Array[Long],
                     tempRows: scala.collection.mutable.Map[Int, Array[Double]],
                     idx: Int): (scala.collection.mutable.Map[Int, Array[Double]], Array[Overhead], Array[Overhead]) = {

    val rows: Array[Long] = mh.getRowAssignments(ID)
    val cols: Array[Long] = mh.getColAssignments(ID)

    val numRows: Long = math.ceil((rows(1) - rows(0) + 1) / rows(2)).toLong
    val numCols: Long = math.ceil((cols(1) - cols(0) + 1) / cols(2)).toLong

    val numElements: Long = numRows * numCols
    val numMessages: Short = math.ceil((numElements * 8.0) / writeMessage.maxBodyLength).toShort

    val numRowsPerMessage: Long = math.ceil(numRows / numMessages).toLong
    var numSentElements: Long = 0l

    var rowStart: Long = 0
    var rowEnd: Long = 0

    var sendOverheads: Array[Overhead] = Array.empty[Overhead]
    var receiveOverheads: Array[Overhead] = Array.empty[Overhead]

    for (m <- 0 until numMessages) {

      val sendStartTime = System.nanoTime

      writeMessage.start(clientID, sessionID, Command.RequestMatrixBlocks)
      writeMessage.writeMatrixID(mh.id)

      rowEnd += (numRowsPerMessage - 1) * rows(2)

      var localRows: Array[Long] = Array.empty[Long]

      rowIndices foreach (rowIndex => {
        localRows = localRows :+ rowIndex
      })

      localRows.sorted

      rowStart = localRows(0) + rows(2) - (localRows(0) % rows(2)) - 1
      rowEnd = localRows.last

      writeMessage.writeLong(rowStart)
      writeMessage.writeLong(rowEnd)
      writeMessage.writeLong(rows(2))
      writeMessage.writeLong(cols(0))
      writeMessage.writeLong(cols(1))
      writeMessage.writeLong(cols(2))

      val (sendBytes, sendTime) = sendMessage
      sendOverheads = sendOverheads :+ new Overhead(0, sendBytes, sendTime, System.nanoTime - sendStartTime)

      val (receiveBytes, receiveTime) = receiveMessage
      val receiveStartTime = System.nanoTime

      val mid: MatrixID = readMessage.readMatrixID

      if (mh.id == mid) {
        val mb: MatrixBlock = readMessage.readMatrixBlock(false)

        for (i <- mb.rows(0) to mb.rows(1) by mb.rows(2)) {
          for (j <- mb.cols(0) to mb.cols(1) by mb.cols(2)) {
            tempRows(i.toInt)(j.toInt) = readMessage.messageBuffer.getDouble
          }
        }
      }

      receiveOverheads = receiveOverheads :+ new Overhead(1, receiveBytes, receiveTime, System.nanoTime - receiveStartTime)
    }

    (tempRows, sendOverheads, receiveOverheads)
  }

  def addIndexedRow(index: Long, length: Long, values: Array[Double]): this.type = {
    writeMessage.writeIndexedRow(index, length, values)

    this
  }

  def addIndexedRow(row: IndexedRow): this.type = {
    writeMessage.writeIndexedRow(row.index.toLong, row.vector.size.toLong, row.vector.toArray)

    this
  }

  def finishSendIndexedRows: Long = {
    sendMessage

    val arrayID: MatrixID = readMessage.readMatrixID
    readMessage.readLong
  }

  def startRequestIndexedRows(id: MatrixID): this.type = {
    writeMessage.start(clientID, sessionID, Command.RequestIndexedRows)
    writeMessage.writeMatrixID(id)

    this
  }

  def requestIndexedRow(index: Long): this.type = {
    writeMessage.writeLong(index)

    this
  }

  def finishRequestIndexedRows: Array[IndexedRow] = {
    sendMessage

    val arrayID: MatrixID = readMessage.readMatrixID

    var rows: Array[IndexedRow] = Array.empty[IndexedRow]

    while (!readMessage.eom)
      rows = rows :+ readMessage.readIndexedRow

    rows
  }

  def startSendMatrixBlocks(id: MatrixID): this.type = {
    writeMessage.start(clientID, sessionID, Command.SendMatrixBlocks)
    writeMessage.writeMatrixID(id)

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

    writeMessage.start(clientID, sessionID, Command.RequestMatrixBlocks)
    writeMessage.writeShort(id)

    this
  }

  def addRequestedMatrixBlock(blockRange: Array[Long]): this.type = {

    blockRange.foreach(i => writeMessage.writeLong(i))

    this
  }

  def getRequestedMatrixBlock(blockRange: Array[Long]): DenseVector = {

    val mb: MatrixBlock = readMessage.readMatrixBlock(true)

    new DenseVector(Array[Double](1.0,2.0,3.0))
  }

  def finishRequestMatrixBlocks: this.type = {
    sendMessage

    this
  }

  override def toString: String = f"Worker-$ID%03d running on $hostname at $address:$port"

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
//  def newMatrixAddRow(handle: MatrixHandle, rowIdx: Long, vals: Matrix[Double]) = {
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
//    val vec = new Matrix[Double](numCols)
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

}
