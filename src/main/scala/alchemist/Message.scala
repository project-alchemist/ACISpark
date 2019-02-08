package alchemist

import java.nio.{Buffer, ByteBuffer, ByteOrder}

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
  var currentDatatype: Byte = Datatype.NoneType.value
  var currentDatatypeCount: Int = 0
  var currentDatatypeCountMax: Int = 0
  var currentDatatypeCountPos: Int = headerLength + 1

  var readPos: Int = headerLength // for reading data
  var writePos: Int = headerLength // for writing data

  def reset(): this.type = {

    clientID = 0
    sessionID = 0
    commandCode = Command.Wait.value
    bodyLength = 0

    currentDatatype = Datatype.NoneType.value
    currentDatatypeCount = 0
    currentDatatypeCountMax = 0
    currentDatatypeCountPos = headerLength

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

  // Reading data
  def readNextDatatype: this.type = {
    currentDatatype = messageBuffer.get(readPos)
    currentDatatypeCount = 0
    currentDatatypeCountMax = ByteBuffer.wrap(messageBuffer.array.slice(readPos + 1, readPos + 5)).order(ByteOrder.BIG_ENDIAN).getInt
    readPos += 5

    this
  }

  def previewNextDatatype: Byte = messageBuffer.get(readPos)

  def previewNextDatatypeCount: Int = ByteBuffer.wrap(messageBuffer.array.slice(readPos+1, readPos+5))
                                                .order(ByteOrder.BIG_ENDIAN)
                                                .getInt

  def getCurrentDatatype(): Byte = currentDatatype

  def getCurrentDatatypeName(): String = Datatype.withValue(currentDatatype).entryName

  def getCurrentDatatypeCount(): Int = currentDatatypeCountMax

  def readByte(): Byte = {
    if (readPos == headerLength | currentDatatypeCount == currentDatatypeCountMax) {
      readNextDatatype
    }

    readPos += 1
    currentDatatypeCount += 1
    messageBuffer.get(readPos - 1)
  }

  def readChar(): Int = {
    if (readPos == headerLength | currentDatatypeCount == currentDatatypeCountMax) {
      readNextDatatype
    }

    readPos += 2
    currentDatatypeCount += 1
    ByteBuffer.wrap(messageBuffer.array.slice(readPos - 2, readPos)).order(ByteOrder.BIG_ENDIAN).getChar
  }

  def readShort(): Short = {
    if (readPos == headerLength | currentDatatypeCount == currentDatatypeCountMax) {
      readNextDatatype
    }

    readPos += 2
    currentDatatypeCount += 1
    ByteBuffer.wrap(messageBuffer.array.slice(readPos - 2, readPos)).order(ByteOrder.BIG_ENDIAN).getShort
  }

  def readInt(): Int = {
    if (readPos == headerLength | currentDatatypeCount == currentDatatypeCountMax) {
      readNextDatatype
    }

    readPos += 4
    currentDatatypeCount += 1
    ByteBuffer.wrap(messageBuffer.array.slice(readPos - 4, readPos)).order(ByteOrder.BIG_ENDIAN).getInt
  }

  def readLong(): Long = {
    if (readPos == headerLength | currentDatatypeCount == currentDatatypeCountMax) {
      readNextDatatype
    }

    readPos += 8
    currentDatatypeCount += 1
    ByteBuffer.wrap(messageBuffer.array.slice(readPos - 8, readPos)).order(ByteOrder.BIG_ENDIAN).getLong
  }

  def readFloat(): Float = {
    if (readPos == headerLength | currentDatatypeCount == currentDatatypeCountMax) {
      readNextDatatype
    }

    readPos += 4
    currentDatatypeCount += 1
    ByteBuffer.wrap(messageBuffer.array.slice(readPos - 4, readPos)).order(ByteOrder.BIG_ENDIAN).getFloat
  }

  def readDouble(): Double = {
    if (readPos == headerLength | currentDatatypeCount == currentDatatypeCountMax) {
      readNextDatatype
    }

    readPos += 8
    currentDatatypeCount += 1
    ByteBuffer.wrap(messageBuffer.array.slice(readPos - 8, readPos)).order(ByteOrder.BIG_ENDIAN).getDouble
  }

  def readDouble(num: Int): Array[Double] = {
    if (readPos == headerLength | currentDatatypeCount == currentDatatypeCountMax) readNextDatatype

    readPos += 8 * num
    currentDatatypeCount += num
    val vec = new Array[Double](num)

    ByteBuffer.wrap(messageBuffer.array.slice(readPos - 8 * num, readPos))
      .order(ByteOrder.BIG_ENDIAN)
      .asDoubleBuffer()
      .get(vec)

    vec
  }

  def readString: String = {

    if (readPos == headerLength | currentDatatypeCount == currentDatatypeCountMax) {
      readNextDatatype
    }

    currentDatatypeCount += 1
    readPos += 4
    val strLength = ByteBuffer.wrap(messageBuffer.array.slice(readPos - 4, readPos))
                              .order(ByteOrder.BIG_ENDIAN).getInt
    readPos += 2 * strLength
    new String(ByteBuffer.wrap(messageBuffer.array.slice(readPos - 2 * strLength, readPos))
                         .order(ByteOrder.BIG_ENDIAN).array(), "utf-16")
  }

  def readParameter: this.type = {
    if (readPos == headerLength | currentDatatypeCount == currentDatatypeCountMax) {
      readNextDatatype
    }

    this
  }

  def readLibraryID: Byte = {
    if (readPos == headerLength | currentDatatypeCount == currentDatatypeCountMax) readNextDatatype

    readPos += 1
    currentDatatypeCount += 1
    messageBuffer.get(readPos - 1)
  }

  def readMatrixID: Short = {
    if (readPos == headerLength | currentDatatypeCount == currentDatatypeCountMax) readNextDatatype

    readPos += 2
    currentDatatypeCount += 1
    ByteBuffer.wrap(messageBuffer.array.slice(readPos - 2, readPos)).order(ByteOrder.BIG_ENDIAN).getShort
  }

  def readMatrixInfo: MatrixHandle = {
    if (readPos == headerLength | currentDatatypeCount == currentDatatypeCountMax) readNextDatatype

    currentDatatypeCount += 1
    readPos += 2
    val matrixID: Short = ByteBuffer.wrap(messageBuffer.array.slice(readPos - 2, readPos))
      .order(ByteOrder.BIG_ENDIAN).getShort
    readPos += 4
    val nameLength: Int = ByteBuffer.wrap(messageBuffer.array.slice(readPos - 4, readPos))
      .order(ByteOrder.BIG_ENDIAN).getInt
    var name: String = " "
    if (nameLength > 0) {
      readPos += 2 * nameLength
      name = new String(ByteBuffer.wrap(messageBuffer.array.slice(readPos - 2 * nameLength, readPos))
                                  .order(ByteOrder.BIG_ENDIAN).array(), "utf-16")
    }
    readPos += 8
    val numRows: Long = ByteBuffer.wrap(messageBuffer.array.slice(readPos - 8, readPos))
      .order(ByteOrder.BIG_ENDIAN).getLong
    readPos += 8
    val numCols: Long = ByteBuffer.wrap(messageBuffer.array.slice(readPos - 8, readPos))
      .order(ByteOrder.BIG_ENDIAN).getLong
    readPos += 1
    val sparse: Byte = messageBuffer.get(readPos - 1)
    readPos += 1
    val layout: Byte = messageBuffer.get(readPos - 1)
    readPos += 1
    val numPartitions: Byte = messageBuffer.get(readPos - 1)
    val rowLayout = new Array[Byte](10)
    for (i <- 0l to numRows) {
      readPos += 1
      messageBuffer.get(readPos - 1)
    }
    new MatrixHandle(matrixID, name, numRows, numCols, sparse, numPartitions, rowLayout)
  }

  // Writing data
  def start(clientID: Short, sessionID: Short, command: Command): this.type = {

    reset
    tempBuffer.asInstanceOf[Buffer].clear
    tempBuffer.putShort(clientID)

    var bbArray = tempBuffer.array
    for (i <- 0 until 2) messageBuffer.put(i, bbArray(i))

    tempBuffer.asInstanceOf[Buffer].clear
    tempBuffer.putShort(sessionID)

    bbArray = tempBuffer.array
    for (i <- 0 until 2) messageBuffer.put(i + 2, bbArray(i))

    messageBuffer.put(4, command.value)

    this
  }

  def addHeader(header: Array[Byte]): this.type = {

    for (i <- 0 until 9) messageBuffer.put(i, header(i))
    readHeader

    this
  }

  def addPacket(packet: Array[Byte], length: Int): this.type = {

    for (i <- 0 until length) messageBuffer.put(writePos + i, packet(i))

    writePos += length

    this
  }

  def putByte(value: Byte, pos: Int): this.type = {

    messageBuffer.put(pos, value)

    this
  }

  def writeByte(value: Byte): this.type = {

    checkDatatype(Datatype.ByteType).putByte(value, writePos)
    writePos += 1

    this
  }

  def putChar(value: Char, pos: Int): this.type = {

    tempBuffer.asInstanceOf[Buffer].clear
    tempBuffer.putChar(value)

    val bbArray = tempBuffer.array
    for (i <- 0 until 2) messageBuffer.put(pos + i, bbArray(i))

    this
  }

  def writeChar(value: Char): this.type = {

    checkDatatype(Datatype.CharType).putChar(value, writePos)
    writePos += 2

    this
  }

  def putShort(value: Short, pos: Int): this.type = {

    tempBuffer.asInstanceOf[Buffer].clear
    tempBuffer.putShort(value)

    val bbArray = tempBuffer.array
    for (i <- 0 until 2) messageBuffer.put(pos + i, bbArray(i))

    this
  }

  def writeShort(value: Short): this.type = {

    checkDatatype(Datatype.ShortType).putShort(value, writePos)
    writePos += 2

    this
  }

  def putInt(value: Int, pos: Int): this.type = {

    tempBuffer.asInstanceOf[Buffer].clear
    tempBuffer.putInt(value)

    val bbArray = tempBuffer.array
    for (i <- 0 until 4) messageBuffer.put(pos + i, bbArray(i))

    this
  }

  def writeInt(value: Int): this.type = {

    checkDatatype(Datatype.IntType).putInt(value, writePos)
    writePos += 4

    this
  }

  def putLong(value: Long, pos: Int): this.type = {

    tempBuffer.asInstanceOf[Buffer].clear
    tempBuffer.putLong(value)

    val bbArray = tempBuffer.array
    for (i <- 0 until 8) messageBuffer.put(pos + i, bbArray(i))

    this
  }

  def writeLong(value: Long): this.type = {

    checkDatatype(Datatype.LongType).putLong(value, writePos)
    writePos += 8

    this
  }

  def putFloat(value: Float, pos: Int): this.type = {

    tempBuffer.asInstanceOf[Buffer].clear
    tempBuffer.putFloat(value)

    val bbArray = tempBuffer.array
    for (i <- 0 until 4) messageBuffer.put(pos + i, bbArray(i))

    this
  }

  def writeFloat(value: Float): this.type = {

    checkDatatype(Datatype.FloatType).putFloat(value, writePos)
    writePos += 4

    this
  }

  def putDouble(value: Double, pos: Int): this.type = {

    tempBuffer.asInstanceOf[Buffer].clear
    tempBuffer.putDouble(value)

    val bbArray = tempBuffer.array
    for (i <- 0 until 8) messageBuffer.put(pos + i, bbArray(i))

    this
  }

  def writeDouble(value: Double): this.type = {

    checkDatatype(Datatype.DoubleType).putDouble(value, writePos)
    writePos += 8

    this
  }

  def putString(value: String, pos: Int): this.type = {

    val stringBuffer = value.getBytes("utf-16")

    for (i <- 2 until stringBuffer.size) messageBuffer.put(pos + i - 2, stringBuffer(i))

    this
  }

  def writeString(value: String): this.type = {

    checkDatatype(Datatype.StringType)
    putInt(value.length, writePos)
    writePos += 4
    putString(value, writePos)
    writePos += 2 * value.length

    this
  }

  def writeMatrixID(value: Short): this.type = {

    checkDatatype(Datatype.MatrixID).putShort(value, writePos)
    writePos += 2

    this
  }

  def writeLibraryID(value: Byte): this.type = {

    checkDatatype(Datatype.LibraryID).putByte(value, writePos)
    writePos += 1

    this
  }

  def writeParameter(): this.type = checkDatatype(Datatype.Parameter)

  // ========================================================================================

  def checkDatatype(datatype: Datatype): this.type = {

    if (currentDatatype != Datatype.IntType.value) {
      currentDatatype = Datatype.IntType.value

      updateDatatypeCount
      putByte(currentDatatype, writePos)

      currentDatatypeCount = 1
      currentDatatypeCountPos = writePos + 1
      writePos += 5
    }
    else currentDatatypeCount += 1

    this
  }

  def updateBodyLength: this.type = {

    bodyLength = writePos - headerLength

    tempBuffer.asInstanceOf[Buffer].clear
    tempBuffer.putInt(bodyLength)

    val bbArray = tempBuffer.array
    for (i <- 0 until 4) messageBuffer.put(i + 5, bbArray(i))

    this
  }

  def updateDatatypeCount: this.type = {

    if (currentDatatypeCount > headerLength) putInt(currentDatatypeCount, currentDatatypeCountPos)

    this
  }

  def finish(): Array[Byte] = {
    updateBodyLength
    updateDatatypeCount

    messageBuffer.array.slice(0, headerLength + bodyLength)
  }

  // ========================================================================================

  def print: this.type = {

    val space: String = "                                              "
    var data: String = ""

    val tt = messageBuffer.array

    val tempClientID: Short = ByteBuffer.wrap(tt.slice(0, 2)).order(ByteOrder.BIG_ENDIAN).getShort
    val tempSessionID: Short = ByteBuffer.wrap(tt.slice(2, 4)).order(ByteOrder.BIG_ENDIAN).getShort
    val tempCommandCode = tt(4)
    val tempBodyLength: Int = ByteBuffer.wrap(tt.slice(5, 9)).order(ByteOrder.BIG_ENDIAN).getInt

    System.out.println()
    System.out.println(s"$space{} ==============================================")
    System.out.println(s"$space Client ID:            $tempClientID")
    System.out.println(s"$space Session ID:           $tempSessionID")
    System.out.println(s"$space Command code:         $tempCommandCode")// (${Command.withValue(tempCommandCode).entryName})
    System.out.println(s"$space Message body length:  $tempBodyLength")
    System.out.println(s"$space ------------------------------------------------")
    System.out.println(" ")

    var i: Int = headerLength

    while (i < headerLength + tempBodyLength) {

      val dataArrayType: Byte = tt(i)
      val dataArrayLength: Int = ByteBuffer.wrap(tt.slice(i + 1, i + 5)).order(ByteOrder.BIG_ENDIAN).getInt

      println(s"$space Datatype (length):    $dataArrayType ($dataArrayLength)")

      data = ""
      i += 5

      for (j <- 0 to dataArrayLength) {
        if (dataArrayType == Datatype.ByteType.value) {
          data = data.concat(s"${tt(i)} ")
          i += 1
        }
        else if (dataArrayType == Datatype.CharType.value) {
          data = data.concat(s" ${ByteBuffer.wrap(tt.slice(i, i + 2)).order(ByteOrder.BIG_ENDIAN).getChar}")
          i += 2
        }
        else if (dataArrayType == Datatype.ShortType.value) {
          data = data.concat(s" ${ByteBuffer.wrap(tt.slice(i, i + 2)).order(ByteOrder.BIG_ENDIAN).getShort} ")
          i += 2
        }
        else if (dataArrayType == Datatype.IntType.value) {
          data = data.concat(s" ${ByteBuffer.wrap(tt.slice(i, i + 4)).order(ByteOrder.BIG_ENDIAN).getInt} ")
          i += 4
        }
        else if (dataArrayType == Datatype.LongType.value) {
          data = data.concat(s" ${ByteBuffer.wrap(tt.slice(i, i + 8)).order(ByteOrder.BIG_ENDIAN).getLong} ")
          i += 8
        }
        else if (dataArrayType == Datatype.FloatType.value) {
          data = data.concat(s" ${ByteBuffer.wrap(tt.slice(i, i + 4)).order(ByteOrder.BIG_ENDIAN).getFloat}")
          i += 4
        }
        else if (dataArrayType == Datatype.DoubleType.value) {
          data = data.concat(s" ${ByteBuffer.wrap(tt.slice(i, i + 8)).order(ByteOrder.BIG_ENDIAN).getDouble}")
          i += 8
        }
        else if (dataArrayType == Datatype.StringType.value) {
          val strLength: Int = ByteBuffer.wrap(tt.slice(i, i + 4)).order(ByteOrder.BIG_ENDIAN).getInt
          i += 4
          if (strLength > 0) {
            data = data.concat(" ")
            data = data.concat(new String(tt.slice(i, i + 2 * strLength), "utf-16"))
            i += 2 * strLength
          }
        }
        else if (dataArrayType == Datatype.LibraryID.value) {
          data = data.concat(s"${tt(i)} ")
          i += 1
        }
        else if (dataArrayType == Datatype.MatrixID.value) {
          data = data.concat(s" ${ByteBuffer.wrap(tt.slice(i, i + 2)).order(ByteOrder.BIG_ENDIAN).getShort}")
          i += 2
        }
        else if (dataArrayType == Datatype.MatrixInfo.value) {
          val matrixID: Short = ByteBuffer.wrap(tt.slice(i, i + 2))
                                          .order(ByteOrder.BIG_ENDIAN).getShort
          i += 2
          val nameLength: Int = ByteBuffer.wrap(tt.slice(i, i + 4))
                                          .order(ByteOrder.BIG_ENDIAN).getInt
          i += 4
          var name: String = " "
          if (nameLength > 0) {
            name = new String(tt.slice(i, i + 2 * nameLength), "utf-16")
            i += 2 * nameLength
          }
          val numRows: Long = ByteBuffer.wrap(tt.slice(i, i + 8)).order(ByteOrder.BIG_ENDIAN).getLong
          i += 8
          val numCols: Long = ByteBuffer.wrap(tt.slice(i, i + 8)).order(ByteOrder.BIG_ENDIAN).getLong
          i += 8
          val sparse: Byte = tt(i)
          i += 1
          val layout: Byte = tt(i)
          i += 1
          val numPartitions: Byte = tt(i)
          val rowLayout = Array[Byte](10)
          for (k <- 0l to numRows) {
            messageBuffer.get(i)
            i += 1
          }
          val mh = new MatrixHandle(matrixID, name, numRows, numCols, sparse, numPartitions, rowLayout)
          mh.meta(true)
          if (j < dataArrayLength-1)
            data = data.concat(s"\n $space                        ")
        }
      }

      System.out.println(s"$space Data:                $data")
      System.out.println(" ")
    }

    System.out.println(s"$space{} ==============================================")

    this
  }
}
