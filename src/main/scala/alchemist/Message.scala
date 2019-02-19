package alchemist

import scala.reflect.ClassTag
import java.nio.{Buffer, ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets

class Message() {

  val headerLength: Int = 10
  var maxBodyLength: Int = 10000000

  val messageBuffer: ByteBuffer = ByteBuffer.allocate(headerLength + maxBodyLength).order(ByteOrder.BIG_ENDIAN)

  var clientID: Short = 0
  var sessionID: Short = 0
  var commandCode: Byte = Command.Wait.value
  var errorCode: Byte = Error.None.value
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
    errorCode = Error.None.value
    bodyLength = 0

    currentDatatype = Datatype.None.value
    currentDatatypeCount = 0
    currentDatatypeCountMax = 0
    currentDatatypeCountPos = headerLength+1

    messageBuffer.asInstanceOf[Buffer].position(headerLength)

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
  def readClientID: Short = messageBuffer.getShort(0)

  def readSessionID: Short = messageBuffer.getShort(2)

  def readCommandCode: Byte = messageBuffer.get(4)

  def readErrorCode: Byte = messageBuffer.get(5)

  def readBodyLength: Int = messageBuffer.getInt(6)

  def readHeader: this.type = {
    clientID = readClientID
    sessionID = readSessionID
    commandCode = readCommandCode
    errorCode = readErrorCode
    bodyLength = readBodyLength
    readPos = headerLength
    writePos = headerLength

    this
  }

  // ======================================== Reading Data =============================================

//  def readNextDatatype: this.type = {
//
//    currentDatatypeCount = 0
//    currentDatatype = getByte
//    currentDatatypeCountMax = getInt
//
//    this
//  }

  def eom: Boolean = {
    if (messageBuffer.asInstanceOf[Buffer].position >= headerLength + bodyLength) return true
    return false
  }

  def previewNextDatatype: Byte = messageBuffer.get(messageBuffer.asInstanceOf[Buffer].position)

  def previewNextDatatypeCount: Int = ByteBuffer.wrap(messageBuffer.array.slice(readPos+1, readPos+5))
                                                .order(ByteOrder.BIG_ENDIAN)
                                                .getInt

  def getCurrentDatatype(): Byte = currentDatatype

  def getCurrentDatatypeLabel(): String = Datatype.withValue(currentDatatype).label

  def getCurrentDatatypeCount(): Int = currentDatatypeCountMax

  def readByte: Byte = {
    try {
      validateDatatype(previewNextDatatype, Datatype.Byte)
      messageBuffer.get
      messageBuffer.get
    }
    catch {
      case e: InconsistentDatatypeException => {
        println(e)
        0.asInstanceOf[Byte]
      }
    }
  }

  def readChar: Char = {
    try {
      validateDatatype(previewNextDatatype, Datatype.Char)
      messageBuffer.get
      messageBuffer.get.asInstanceOf[Char]
    }
    catch {
      case e: InconsistentDatatypeException => {
        println(e)
        0.asInstanceOf[Char]
      }
    }
  }

  def readShort: Short = {
    try {
      validateDatatype(previewNextDatatype, Datatype.Short)
      messageBuffer.get
      messageBuffer.getShort
    }
    catch {
      case e: InconsistentDatatypeException => {
        println(e)
        0.asInstanceOf[Short]
      }
    }
  }

  def readInt: Int = {
    try {
      validateDatatype(previewNextDatatype, Datatype.Int)
      messageBuffer.get
      messageBuffer.getInt
    }
    catch {
      case e: InconsistentDatatypeException => {
        println(e)
        0
      }
    }
  }

  def readLong: Long = {
    try {
      validateDatatype(previewNextDatatype, Datatype.Long)
      messageBuffer.get
      messageBuffer.getLong
    }
    catch {
      case e: InconsistentDatatypeException => {
        println(e)
        0.asInstanceOf[Long]
      }
    }
  }

  def readFloat: Float = {
    try {
      validateDatatype(previewNextDatatype, Datatype.Float)
      messageBuffer.get
      messageBuffer.getFloat
    }
    catch {
      case e: InconsistentDatatypeException => {
        println(e)
        0.0.asInstanceOf[Float]
      }
    }
  }

  def readDouble: Double = {
    try {
      validateDatatype(previewNextDatatype, Datatype.Double)
      messageBuffer.get
      messageBuffer.getDouble
    }
    catch {
      case e: InconsistentDatatypeException => {
        println(e)
        0.0
      }
    }
  }

  def readString: String = {
    try {
      validateDatatype(previewNextDatatype, Datatype.String)
      messageBuffer.get
      getString
    }
    catch {
      case e: InconsistentDatatypeException => {
        println(e)
        ""
      }
    }
  }

  def readParameter: Unit = {
    try {
      validateDatatype(previewNextDatatype, Datatype.Parameter)
      messageBuffer.get
    }
    catch {
      case e: InconsistentDatatypeException => println(e)
    }
  }

  def readLibraryID: Byte = {
    try {
      validateDatatype(previewNextDatatype, Datatype.LibraryID)
      messageBuffer.get
      messageBuffer.get
    }
    catch {
      case e: InconsistentDatatypeException => {
        println(e)
        0.asInstanceOf[Byte]
      }
    }
  }

  def getArrayID: Short = {
    messageBuffer.get
  }

  def getWorkerID: Short = {
    messageBuffer.getShort
  }

  def readArrayID: Short = {
    try {
      validateDatatype(previewNextDatatype, Datatype.ArrayID)
      messageBuffer.get
      getArrayID
    }
    catch {
      case e: InconsistentDatatypeException => {
        println(e)
        0.asInstanceOf[Short]
      }
    }
  }

  def readWorkerID: Short = {
    try {
      validateDatatype(previewNextDatatype, Datatype.WorkerID)
      messageBuffer.get
      getWorkerID
    }
    catch {
      case e: InconsistentDatatypeException => {
        println(e)
        0.asInstanceOf[Short]
      }
    }
  }

  def getWorkerInfo: WorkerInfo = {
    val workerID: Short = getWorkerID
    val hostname: String = getString
    val address: String = getString
    val port: Short = messageBuffer.getShort
    val groupID: Short = messageBuffer.getShort

    new WorkerInfo(workerID, hostname, address, port, groupID)
  }

  def readWorkerInfo: WorkerInfo = {
    try {
      validateDatatype(messageBuffer.get, Datatype.WorkerInfo)
      getWorkerInfo
    }
    catch {
      case e: InconsistentDatatypeException => {
        println(e)
        messageBuffer.asInstanceOf[Buffer].position(messageBuffer.asInstanceOf[Buffer].position - 1)
        new WorkerInfo
      }
    }
  }

  def getByteArray(length: Int): Array[Byte] = {
    val bb: Array[Byte] = ByteBuffer.allocate(length).order(ByteOrder.BIG_ENDIAN).array
    messageBuffer.get(bb, 0, length)
    bb
  }

  def getString: String = {
    val strLength: Short = messageBuffer.getShort
    new String(getByteArray(strLength.asInstanceOf[Int]), StandardCharsets.UTF_8)
  }

  def getArrayInfo: ArrayHandle = {
    val ID: Short = getArrayID
    val nameLength: Int = messageBuffer.getInt
    val name: String = getString
    val numRows: Long = messageBuffer.getLong
    val numCols: Long = messageBuffer.getLong
    val sparse: Byte = messageBuffer.get
    val layout: Byte = messageBuffer.get
    val numPartitions: Byte = messageBuffer.get
    val workerLayout: Array[Byte] = getByteArray(numPartitions)

    new ArrayHandle(ID, name, numRows, numCols, sparse, numPartitions, workerLayout)
  }

  def readArrayInfo: ArrayHandle = {
    try {
      validateDatatype(messageBuffer.get, Datatype.ArrayInfo)
      getArrayInfo
    }
    catch {
      case e: InconsistentDatatypeException => {
        println(e)
        messageBuffer.asInstanceOf[Buffer].position(messageBuffer.asInstanceOf[Buffer].position - 1)
        new ArrayHandle
      }
    }
  }

  def readArrayBlockFloat: ArrayBlockFloat = {
    try {
      validateDatatype(messageBuffer.get, Datatype.ArrayBlockFloat)
      val numDims: Int = messageBuffer.get.toInt

      val nnz: Int = messageBuffer.getLong.toInt
      val dims = Array.ofDim[Long](numDims, 3)
      for (i <- 0 until numDims)
        for (j <- 0 until 3)
          dims(i)(j) = messageBuffer.getLong

      val data = Array.fill[Float](nnz)(0.0.asInstanceOf[Float])
      (0 until nnz).foreach(i => data(i) = messageBuffer.getFloat)

      new ArrayBlockFloat(dims, data)
    }
    catch {
      case e: InconsistentDatatypeException => {
        println(e)
        messageBuffer.asInstanceOf[Buffer].position(messageBuffer.asInstanceOf[Buffer].position - 1)
        new ArrayBlockFloat
      }
    }

  }

  def readArrayBlockDouble: ArrayBlockDouble = {
    try {
      validateDatatype(messageBuffer.get, Datatype.ArrayBlockDouble)
      val numDims: Int = messageBuffer.get.toInt

      val nnz: Int = messageBuffer.getLong.toInt
      val dims = Array.ofDim[Long](numDims, 3)
      for (i <- 0 until numDims)
        for (j <- 0 until 3)
          dims(i)(j) = messageBuffer.getLong

      val data = Array.fill[Double](nnz)(0.0)
      (0 until nnz).foreach(i => data(i) = messageBuffer.getDouble)

      new ArrayBlockDouble(dims, data)
    }
    catch {
      case e: InconsistentDatatypeException => {
        println(e)
        messageBuffer.asInstanceOf[Buffer].position(messageBuffer.asInstanceOf[Buffer].position - 1)
        new ArrayBlockDouble
      }
    }

  }

  // ========================================= Writing Data =========================================

  def start(clientID: Short, sessionID: Short, command: Command): this.type = {
    reset
    messageBuffer.putShort(0, clientID)
                 .putShort(2, sessionID)
                 .put(4, command.value)
                 .put(5, Error.None.value)

    this
  }


  def addHeader(header: Array[Byte]): this.type = {

    messageBuffer.asInstanceOf[Buffer].position(0)
    messageBuffer.put(header.slice(0, headerLength))
    messageBuffer.asInstanceOf[Buffer].position(headerLength)
    readHeader
  }

  def addPacket(packet: Array[Byte], length: Int): this.type = {

    messageBuffer.put(packet.slice(0, length))

    this
  }

  // -------------------------------- Write data types into buffer --------------------------------

  def writeByte(value: Byte): this.type = {
    messageBuffer.put(Datatype.Byte.value)
                 .put(value)

    this
  }

  def writeChar(value: Char): this.type = {
    messageBuffer.put(Datatype.Char.value)
                 .put(value.toByte)

    this
  }

  def writeShort(value: Short): this.type = {
    messageBuffer.put(Datatype.Short.value)
                 .putShort(value)

    this
  }

  def writeInt(value: Int): this.type = {
    messageBuffer.put(Datatype.Int.value)
                 .putInt(value)

    this
  }

  def writeLong(value: Long): this.type = {
    messageBuffer.put(Datatype.Long.value)
                 .putLong(value)

    this
  }

  def writeFloat(value: Float): this.type = {
    messageBuffer.put(Datatype.Float.value)
                 .putFloat(value)

    this
  }

  def writeDouble(value: Double): this.type = {
    messageBuffer.put(Datatype.Double.value)
                 .putDouble(value)

    this
  }

  def writeString(value: String): this.type = {
    messageBuffer.put(Datatype.String.value)
                 .putShort(value.length.asInstanceOf[Short])
                 .put(value.getBytes(StandardCharsets.UTF_8).array)

    this
  }

  def writeLibraryID(value: Byte): this.type = {
    messageBuffer.put(Datatype.LibraryID.value)
                 .put(value)

    this
  }

  def writeArrayID(value: Short): this.type = {
    messageBuffer.put(Datatype.ArrayID.value)
                 .putShort(value)

    this
  }

  def writeArrayBlockFloat(block: ArrayBlockFloat): this.type = {
    messageBuffer.put(Datatype.ArrayBlockFloat.value)
      .put(block.dims.size.toByte)
      .putLong(block.nnz)
    block.dims.foreach(d1 => d1.foreach(d2 => messageBuffer.putLong(d2)))
    block.data.foreach(e => messageBuffer.putFloat(e))

    this
  }

  def writeArrayBlockDouble(block: ArrayBlockDouble): this.type = {
    messageBuffer.put(Datatype.ArrayBlockDouble.value)
                 .put(block.dims.size.toByte)
                 .putLong(block.nnz)
    block.dims.foreach(d1 => d1.foreach(d2 => messageBuffer.putLong(d2)))
    block.data.foreach(e => messageBuffer.putDouble(e))

    this
  }

  def writeParameter(): this.type = {
    messageBuffer.put(Datatype.Parameter.value)

    this
  }

  // ========================================================================================

  @throws(classOf[InconsistentDatatypeException])
  def validateDatatype(code: Byte, datatype: Datatype): Unit = {
    if (code != datatype.value) {
      val message: String = s"Actual datatype ${Datatype.withValue(code).label} does not match expected datatype ${datatype.label}"
      throw new InconsistentDatatypeException(message)
    }
  }

  def checkDatatype(datatype: Datatype): this.type = {

    if (currentDatatype != datatype.value) {
      currentDatatype = datatype.value

      updateDatatypeCount
      messageBuffer.put(currentDatatype)

      currentDatatypeCount = 1
      currentDatatypeCountPos = writePos
      writePos += 4

      messageBuffer.asInstanceOf[Buffer].position(messageBuffer.asInstanceOf[Buffer].position+4)
    }
    else currentDatatypeCount += 1

    this
  }

  def updateBodyLength: this.type = {
    bodyLength = messageBuffer.asInstanceOf[Buffer].position - headerLength
    messageBuffer.putInt(6, bodyLength)

    this
  }

  def updateDatatypeCount: this.type = {
    messageBuffer.putInt(currentDatatypeCountPos, currentDatatypeCount)

    this
  }

  def finish(): Array[Byte] = {
    updateBodyLength

    messageBuffer.array.slice(0, headerLength + bodyLength)
  }

  // ========================================================================================

  def print: this.type = {

    val space: String = "                                              "

    readHeader
    messageBuffer.asInstanceOf[Buffer].position(headerLength)

    System.out.println()
    System.out.println(s"$space ==================================================================")
    System.out.println(s"$space Client ID:                 $clientID")
    System.out.println(s"$space Session ID:                $sessionID")
    System.out.println(s"$space Command code:              $commandCode (${Command.withValue(commandCode).label})")
    System.out.println(s"$space Error code:                $errorCode (${Error.withValue(errorCode).label})")
    System.out.println(s"$space Message body length:       $bodyLength")
    System.out.println(s"$space ------------------------------------------------------------------")
    System.out.println(" ")

    while (!eom) {
      currentDatatype = previewNextDatatype

      var data: String = f"$space ${Datatype.withValue(currentDatatype).label}%-20s      "

      currentDatatype match {
        case Datatype.Byte.value => data = data.concat(s" $readByte ")
        case Datatype.Char.value => data = data.concat(s" $readChar ")
        case Datatype.Short.value => data = data.concat(s" $readShort ")
        case Datatype.Int.value => data = data.concat(s" $readInt ")
        case Datatype.Long.value => data = data.concat(s" $readLong ")
        case Datatype.Float.value => data = data.concat(s" $readFloat ")
        case Datatype.Double.value => data = data.concat(s" $readDouble ")
        case Datatype.String.value => data = data.concat(s" $readString ")
        case Datatype.LibraryID.value => data = data.concat(s" $readLibraryID ")
        case Datatype.WorkerID.value => data = data.concat(s" $readWorkerID ")
        case Datatype.WorkerInfo.value => data = data.concat(s" ${readWorkerInfo.toString}")
        case Datatype.ArrayID.value => data = data.concat(s" ${readArrayID} ")
        case Datatype.ArrayInfo.value => data = data.concat(s" ${readArrayInfo.toString}")
        case Datatype.ArrayBlockFloat.value => data = data.concat(s" ${readArrayBlockFloat.toString(space + "                            ")}")
        case Datatype.ArrayBlockDouble.value => data = data.concat(s" ${readArrayBlockDouble.toString(space + "                            ")}")
      }

      System.out.println(s"$data")
    }

    messageBuffer.asInstanceOf[Buffer].position(headerLength)

    System.out.println(s"$space ==================================================================")

    this
  }
}
