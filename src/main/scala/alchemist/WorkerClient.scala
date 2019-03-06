package alchemist

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.distributed.IndexedRow

class WorkerClient(_ID: Short, _hostname: String, _address: String, _port: Short) extends Client {

  {
    ID = _ID
    hostname = _hostname
    address = _address
    port = _port
  }

  def startSendIndexedRows(id: ArrayID): this.type = {
    writeMessage.start(clientID, sessionID, Command.SendIndexedRows)
    writeMessage.writeArrayID(id)

    this
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

    val arrayID: ArrayID = readMessage.readArrayID
    readMessage.readLong
  }

  def startRequestIndexedRows(id: ArrayID): this.type = {
    writeMessage.start(clientID, sessionID, Command.RequestIndexedRows)
    writeMessage.writeArrayID(id)

    this
  }

  def requestIndexedRow(index: Long): this.type = {
    writeMessage.writeLong(index)

    this
  }

  def finishRequestIndexedRows: Array[IndexedRow] = {
    sendMessage

    val arrayID: ArrayID = readMessage.readArrayID

    var rows: Array[IndexedRow] = Array.empty[IndexedRow]

    while (!readMessage.eom)
      rows = rows :+ readMessage.readIndexedRow

    rows
  }

  def startSendArrayBlocks(id: ArrayID): this.type = {
    writeMessage.start(clientID, sessionID, Command.SendArrayBlocks)
    writeMessage.writeArrayID(id)

    this
  }

  def addSendArrayBlock(blockRange: Array[Long], block: Array[Double]): this.type = {

    blockRange.foreach(i => writeMessage.writeLong(i))
    block.foreach(v => writeMessage.writeDouble(v))

    this
  }

  def finishSendArrayBlocks: this.type = {
    sendMessage

    this
  }

  def startRequestArrayBlocks(id: Short): this.type = {
    writeMessage.start(clientID, sessionID, Command.RequestArrayBlocks)
    writeMessage.writeShort(id)

    this
  }

  def addRequestedArrayBlock(blockRange: Array[Long]): this.type = {

    blockRange.foreach(i => writeMessage.writeLong(i))

    this
  }

  def getRequestedArrayBlock(blockRange: Array[Long]): DenseVector = {

    val b = readMessage.readArrayBlockDouble

    new DenseVector(Array[Double](1.0,2.0,3.0))
  }

  def finishRequestArrayBlocks: this.type = {
    sendMessage

    this
  }

  def close: this.type = disconnectFromAlchemist

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
//  def newArrayAddRow(handle: ArrayHandle, rowIdx: Long, vals: Array[Double]) = {
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
//  def newArrayPartitionComplete(handle: ArrayHandle) = {
//    val outbuf = beginOutput(4)
//    outbuf.putInt(0x2)  // typeCode = partitionComplete
//    sendMessage(outbuf)
//  }
//
//  def getIndexedRowArrayRow(handle: ArrayHandle, rowIndex: Long, numCols: Int) : DenseVector = {
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
//  def getIndexedRowArrayPartitionComplete(handle: ArrayHandle) = {
//    val outbuf = beginOutput(4)
//    println(s"Finished getting rows on worker")
//    outbuf.putInt(0x4) // typeCode = doneGettingRows
//    sendMessage(outbuf)
//  }

}
