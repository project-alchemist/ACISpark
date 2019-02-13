package alchemist

import scala.reflect.ClassTag
import java.nio.{Buffer, ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets

class Message() {

  val headerLength: Int = 9
  var maxBodyLength: Int = 10000000

  val messageBuffer: ByteBuffer = ByteBuffer.allocate(headerLength + maxBodyLength).order(ByteOrder.BIG_ENDIAN)
  val tempBuffer: ByteBuffer = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN)

  var clientID: Short = 0
  var sessionID: Short = 0
  var commandCode: Byte = Command.Wait.value
  var bodyLength: Int = 0

  // For writing data
  var currentDatatype: Byte = Datatype.None.value
  var currentDatatypeCount: Int = 0
  var currentDatatypeCountMax: Int = 0
  var currentDatatypeCountPos: Int = headerLength+1

  var readPos: Int = headerLength     // for reading data
  var writePos: Int = headerLength    // for writing body data

  def reset(): this.type = {

    clientID = 0
    sessionID = 0
    commandCode = Command.Wait.value
    bodyLength = 0

    currentDatatype = Datatype.None.value
    currentDatatypeCount = 0
    currentDatatypeCountMax = 0
    currentDatatypeCountPos = headerLength+1

    readPos = headerLength
    writePos = headerLength

    this
  }

  // Utility methods
  def getHeaderLength: Int = headerLength

  def getCommandCode: Byte = commandCode

  def getBodyLength: Int = bodyLength

  // Return raw byte array
  def get: this.type = {
    updateBodyLength.updateDatatypeCount.messageBuffer.array.slice(0, headerLength + bodyLength)

    this
  }

  // Reading header
  def readClientID: Short = ByteBuffer.wrap(messageBuffer.array.slice(0, 2)).order(ByteOrder.BIG_ENDIAN).getShort

  def readSessionID: Short = ByteBuffer.wrap(messageBuffer.array.slice(2, 4)).order(ByteOrder.BIG_ENDIAN).getShort

  def readCommandCode: Byte = messageBuffer.get(4)

  def readBodyLength: Int = ByteBuffer.wrap(messageBuffer.array.slice(5, 9)).order(ByteOrder.BIG_ENDIAN).getInt

  def readHeader: this.type = {
    clientID = readClientID
    sessionID = readSessionID
    commandCode = readCommandCode
    bodyLength = readBodyLength
    readPos = headerLength
    writePos = headerLength

    this
  }

  // ======================================== Reading Data =============================================

  def readNextDatatype: this.type = {

    currentDatatypeCount = 0
    currentDatatype = getByte
    currentDatatypeCountMax = getInt

    this
  }

  def eom: Boolean = {
    if (readPos >= headerLength + bodyLength) return true
    return false
  }

  def previewNextDatatype: Byte = messageBuffer.get(readPos)

  def previewNextDatatypeCount: Int = ByteBuffer.wrap(messageBuffer.array.slice(readPos+1, readPos+5))
                                                .order(ByteOrder.BIG_ENDIAN)
                                                .getInt

  def getCurrentDatatype(): Byte = currentDatatype

  def getCurrentDatatypeLabel(): String = Datatype.withValue(currentDatatype).label

  def getCurrentDatatypeCount(): Int = currentDatatypeCountMax

  def getByte: Byte = {

    readPos += 1
    messageBuffer.get(readPos-1)
  }

  def getByteArray(len: Int): Array[Byte] = {

    readPos += len
    ByteBuffer.wrap(messageBuffer.array.slice(readPos-len, readPos)).order(ByteOrder.BIG_ENDIAN).array()
  }

  def getChar: Char = {

    readPos += 1
    ByteBuffer.wrap(Array[Byte](0, messageBuffer.get(readPos-1))).order(ByteOrder.BIG_ENDIAN).getChar
  }

  def getShort: Short = {

    readPos += 2
    ByteBuffer.wrap(messageBuffer.array.slice(readPos-2, readPos)).order(ByteOrder.BIG_ENDIAN).getShort
  }

  def getInt: Int = {

    readPos += 4
    ByteBuffer.wrap(messageBuffer.array.slice(readPos-4, readPos)).order(ByteOrder.BIG_ENDIAN).getInt
  }

  def getLong: Long = {

    readPos += 8
    ByteBuffer.wrap(messageBuffer.array.slice(readPos-8, readPos)).order(ByteOrder.BIG_ENDIAN).getLong
  }

  def getFloat: Float = {

    readPos += 4
    ByteBuffer.wrap(messageBuffer.array.slice(readPos-4, readPos)).order(ByteOrder.BIG_ENDIAN).getFloat
  }

  def getDouble: Double = {

    readPos += 8
    ByteBuffer.wrap(messageBuffer.array.slice(readPos-8, readPos)).order(ByteOrder.BIG_ENDIAN).getDouble
  }

  def getString: String = {

    val strLength: Int = getInt
    new String(getByteArray(strLength), StandardCharsets.UTF_8)
  }

  def readByte(): Byte = {
    if (readPos == headerLength | currentDatatypeCount == currentDatatypeCountMax) readNextDatatype

    currentDatatypeCount += 1

    getByte
  }

  def readChar(): Char = {
    if (readPos == headerLength | currentDatatypeCount == currentDatatypeCountMax) readNextDatatype

    currentDatatypeCount += 1

    getChar
  }

  def readShort(): Short = {
    if (readPos == headerLength | currentDatatypeCount == currentDatatypeCountMax) readNextDatatype

    currentDatatypeCount += 1

    getShort
  }

  def readInt(): Int = {
    if (readPos == headerLength | currentDatatypeCount == currentDatatypeCountMax) readNextDatatype

    currentDatatypeCount += 1

    getInt
  }

  def readLong(): Long = {
    if (readPos == headerLength | currentDatatypeCount == currentDatatypeCountMax) readNextDatatype

    currentDatatypeCount += 1

    getLong
  }

  def readFloat(): Float = {
    if (readPos == headerLength | currentDatatypeCount == currentDatatypeCountMax) readNextDatatype

    currentDatatypeCount += 1

    getFloat
  }

  def readDouble(): Double = {
    if (readPos == headerLength | currentDatatypeCount == currentDatatypeCountMax) readNextDatatype

    currentDatatypeCount += 1

    getDouble
  }

//  def readDouble(num: Int): Array[Double] = {
//    if (readPos == headerLength | currentDatatypeCount == currentDatatypeCountMax) readNextDatatype
//
//    readPos += 8 * num
//    currentDatatypeCount += num
//    val vec = new Array[Double](num)
//
//    ByteBuffer.wrap(messageBuffer.array.slice(readPos - 8 * num, readPos))
//      .order(ByteOrder.BIG_ENDIAN)
//      .asDoubleBuffer()
//      .get(vec)
//
//    vec
//  }

  def readString: String = {

    if (readPos == headerLength | currentDatatypeCount == currentDatatypeCountMax) readNextDatatype

    currentDatatypeCount += 1

    getString
  }

  def readParameter: this.type = {
    if (readPos == headerLength | currentDatatypeCount == currentDatatypeCountMax) readNextDatatype

    this
  }

  def readLibraryID: Byte = {
    if (readPos == headerLength | currentDatatypeCount == currentDatatypeCountMax) readNextDatatype

    currentDatatypeCount += 1

    getByte
  }

  def readArrayID: Short = {
    if (readPos == headerLength | currentDatatypeCount == currentDatatypeCountMax) readNextDatatype

    currentDatatypeCount += 1

    getShort
  }

  def readArrayInfo: ArrayHandle = {
    if (readPos == headerLength | currentDatatypeCount == currentDatatypeCountMax) readNextDatatype

    currentDatatypeCount += 1

    getArrayInfo
  }

  def getArrayInfo: ArrayHandle = {
    val matrixID: Short = getShort
    val nameLength: Int = getInt
    val name: String = getString
    val numRows: Long = getLong
    val numCols: Long = getLong
    val sparse: Byte = getByte
    val layout: Byte = getByte
    val numPartitions: Byte = getByte
    val workerLayout: Array[Byte] = getByteArray(numPartitions)

    new ArrayHandle(matrixID, name, numRows, numCols, sparse, numPartitions, workerLayout)
  }

//  def getDoubleArray(pos: Long, length: Long): Array[Double] = {
//    val vec = new Array[Double](num)
//
//    ByteBuffer.wrap(ByteBuffer.array.slice(readPos - 8 * blockSize, readPos))
//      .order(ByteOrder.BIG_ENDIAN)
//      .asDoubleBuffer()
//      .get(vec)
//  }

  def fill2DArray[T : ClassTag](x: Array[Array[T]], rows: Array[Int], cols: Array[Int]): this.type = {
    x match {
      case _: Array[Array[Double]] => {
        for (i <- rows(0) to rows(1) by rows(2))
          for (j <- cols(0) to cols(1) by cols(2))
            x(i.toInt)(j.toInt) = getDouble
      }
      case _: Array[Array[Int]] => {
        for (i <- rows(0) to rows(1) by rows(2))
          for (j <- cols(0) to cols(1) by cols(2))
            x(i.toInt)(j.toInt) = getInt
      }
    }

    this
  }

  def getArrayBlock[T : ClassTag](): ArrayBlock[T] = {
    val numDims: Int = getByte.toInt

    val nnz: Int = getLong.toInt
    val dims = Array.ofDim[Long](numDims, 3)
    for (i <- 0 until numDims)
      for (j <- 0 until 3)
        dims(i)(j) = getLong

    val data = Array.fill[T](nnz)(0.0.asInstanceOf[T])
    (0 until nnz).foreach(i => data(i) = getDouble.asInstanceOf[T])

    new ArrayBlock[T](dims, data)
  }

  def readArrayBlock[T : ClassTag](): ArrayBlock[T] = {

    if (readPos == headerLength | currentDatatypeCount == currentDatatypeCountMax) readNextDatatype
    currentDatatypeCount += 1

    getArrayBlock[T]
  }


  // ========================================= Writing Data =========================================

  def start(clientID: Short, sessionID: Short, command: Command): this.type = reset.putShort(clientID, 0)
                                                                                   .putShort(sessionID, 2)
                                                                                   .putByte(command.value, 4)

  def addHeader(header: Array[Byte]): this.type = {

    for (i <- 0 until 9) messageBuffer.put(i, header(i))
    readHeader
  }

  def addPacket(packet: Array[Byte], length: Int): this.type = {

    for (i <- 0 until length) messageBuffer.put(writePos + i, packet(i))

    writePos += length

    this
  }

  // --------------------------------- Put data types into buffer ---------------------------------

  def putByte(value: Byte, pos: Int = writePos, buffer: ByteBuffer = messageBuffer): this.type = {

    buffer.put(pos, value)
    if (pos == writePos) writePos += 1

    this
  }

  def putChar(value: Char, pos: Int = writePos, buffer: ByteBuffer = messageBuffer): this.type = {

    buffer.put(pos, value.toByte)
    if (pos == writePos) writePos += 1

    this
  }

  def putShort(value: Short, pos: Int = writePos, buffer: ByteBuffer = messageBuffer): this.type = {

    tempBuffer.asInstanceOf[Buffer].clear
    val bb = tempBuffer.order(ByteOrder.BIG_ENDIAN).putShort(value).array

    for (i <- 0 until 2) buffer.put(pos + i, bb(i))
    if (pos == writePos) writePos += 2

    this
  }

  def putInt(value: Int, pos: Int = writePos, buffer: ByteBuffer = messageBuffer): this.type = {

    tempBuffer.asInstanceOf[Buffer].clear
    val bb = tempBuffer.order(ByteOrder.BIG_ENDIAN).putInt(value).array

    for (i <- 0 until 4) buffer.put(pos + i, bb(i))
    if (pos == writePos) writePos += 4

    this
  }

  def putLong(value: Long, pos: Int = writePos, buffer: ByteBuffer = messageBuffer): this.type = {

    tempBuffer.asInstanceOf[Buffer].clear
    val bb = tempBuffer.order(ByteOrder.BIG_ENDIAN).putLong(value).array

    for (i <- 0 until 8) buffer.put(pos + i, bb(i))
    if (pos == writePos) writePos += 8

    this
  }

  def putFloat(value: Float, pos: Int = writePos, buffer: ByteBuffer = messageBuffer): this.type = {

    tempBuffer.asInstanceOf[Buffer].clear
    val bb = tempBuffer.order(ByteOrder.BIG_ENDIAN).putFloat(value).array

    for (i <- 0 until 4) messageBuffer.put(pos + i, bb(i))
    if (pos == writePos) writePos += 4

    this
  }

  def putDouble(value: Double, pos: Int = writePos, buffer: ByteBuffer = messageBuffer): this.type = {

    tempBuffer.asInstanceOf[Buffer].clear
    val bb = tempBuffer.order(ByteOrder.BIG_ENDIAN).putDouble(value).array

    for (i <- 0 until 8) buffer.put(pos + i, bb(i))
    if (pos == writePos) writePos += 8

    this
  }

  def putString(value: String, pos: Int = writePos, buffer: ByteBuffer = messageBuffer): this.type = {

    val stringBuffer = value.getBytes(StandardCharsets.UTF_8)

    for (i <- 0 until stringBuffer.size) buffer.put(pos + i, stringBuffer(i))
    if (pos == writePos) writePos += stringBuffer.size

    this
  }

  def putArrayBlock[T : ClassTag](block: ArrayBlock[T]): this.type = {

    putByte(block.dims.size.toByte)
    putLong(block.nnz)
    block.dims.foreach(d1 => d1.foreach(d2 => putLong(d2)))
    block.data.foreach(e => putDouble(e.asInstanceOf[Double]))

    this
  }

  def writeByte(value: Byte): this.type = checkDatatype(Datatype.Byte).putByte(value)

  def writeChar(value: Char): this.type = checkDatatype(Datatype.Char).putChar(value)

  def writeShort(value: Short): this.type = checkDatatype(Datatype.Short).putShort(value)

  def writeInt(value: Int): this.type = checkDatatype(Datatype.Int).putInt(value)

  def writeLong(value: Long): this.type = checkDatatype(Datatype.Long).putLong(value)

  def writeFloat(value: Float): this.type = checkDatatype(Datatype.Float).putFloat(value)

  def writeDouble(value: Double): this.type = checkDatatype(Datatype.Double).putDouble(value)

  def writeString(value: String): this.type = {
    println(s"wr str $value")

    checkDatatype(Datatype.String).putInt(value.length)
      .putString(value)
  }

  def writeLibraryID(value: Byte): this.type = checkDatatype(Datatype.LibraryID).putByte(value)

  def writeArrayID(value: Short): this.type = checkDatatype(Datatype.ArrayID).putShort(value)

  def writeArrayBlock[T : ClassTag](block: ArrayBlock[T]): this.type = checkDatatype(Datatype.ArrayBlock).putArrayBlock(block)

  def writeParameter(): this.type = checkDatatype(Datatype.Parameter)

  // ========================================================================================

  def checkDatatype(datatype: Datatype): this.type = {

    if (currentDatatype != datatype.value) {
      currentDatatype = datatype.value

      updateDatatypeCount.putByte(currentDatatype)

      currentDatatypeCount = 1
      currentDatatypeCountPos = writePos
      writePos += 4
    }
    else currentDatatypeCount += 1

    this
  }

  def updateBodyLength: this.type = {
    bodyLength = writePos - headerLength
    putInt(bodyLength, 5)
  }

  def updateDatatypeCount: this.type = putInt(currentDatatypeCount, currentDatatypeCountPos)

  def finish(): Array[Byte] = {
    updateBodyLength.updateDatatypeCount

    messageBuffer.array.slice(0, headerLength + bodyLength)
  }

  // ========================================================================================

  def print: this.type = {

    val space: String = "                                              "
    var data: String = ""

    readHeader

    System.out.println()
    System.out.println(s"$space ==================================================================")
    System.out.println(s"$space Client ID:            $clientID")
    System.out.println(s"$space Session ID:           $sessionID")
    System.out.println(s"$space Command code:         $commandCode (${Command.withValue(commandCode).label})")
    System.out.println(s"$space Message body length:  $bodyLength")
    System.out.println(s"$space ------------------------------------------------------------------")
    System.out.println(" ")

    while (!eom) {
      readNextDatatype

      println(s"$space Datatype (length):    ${Datatype.withValue(currentDatatype).label} ($currentDatatypeCountMax)")

      data = ""

      for (j <- 0 until currentDatatypeCountMax) {
        currentDatatype match {
          case Datatype.Byte.value => data = data.concat(s" ${getByte} ")
          case Datatype.Char.value => data = data.concat(s" ${getChar} ")
          case Datatype.Short.value => data = data.concat(s" ${getShort} ")
          case Datatype.Int.value => data = data.concat(s" ${getInt} ")
          case Datatype.Long.value => data = data.concat(s" ${getLong} ")
          case Datatype.Float.value => data = data.concat(s" ${getFloat} ")
          case Datatype.Double.value => data = data.concat(s" ${getDouble} ")
          case Datatype.String.value => data = data.concat(s" ${getString} ")
          case Datatype.LibraryID.value => data = data.concat(s" ${getByte} ")
          case Datatype.ArrayID.value => data = data.concat(s" ${getShort} ")
          case Datatype.ArrayInfo.value => data = data.concat(s" ${getArrayInfo.toString}")
          case Datatype.ArrayBlock.value => data = data.concat(s" ${getArrayBlock[Double].toString(space + "                       ")}")
        }
      }

      System.out.println(s"$space Data:                $data\n")
    }

    reset

    System.out.println(s"$space ==================================================================")

    this
  }
}
