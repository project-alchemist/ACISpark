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

  def reset(): this.type = {

    clientID = 0
    sessionID = 0
    commandCode = Command.Wait.value
    errorCode = Error.None.value
    bodyLength = 0

    messageBuffer.asInstanceOf[Buffer].position(headerLength)

    this
  }

  // Utility methods
  def getHeaderLength: Int = headerLength

  def getCommandCode: Byte = commandCode

  def getBodyLength: Int = bodyLength

  // Return raw byte array
  def get: this.type = {
    updateBodyLength.messageBuffer.array.slice(0, headerLength + bodyLength)

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

    this
  }

  // ======================================== Reading Data =============================================

  def eom: Boolean = {
    if (messageBuffer.asInstanceOf[Buffer].position >= headerLength + bodyLength) return true
    return false
  }

  def previewNextDatatype: Byte = messageBuffer.get(messageBuffer.asInstanceOf[Buffer].position)

  def getArrayID: Short = {
    messageBuffer.get
  }

  def getWorkerID: Short = {
    messageBuffer.getShort
  }

  def getWorkerInfo: WorkerInfo = {
    val workerID: Short = getWorkerID
    val hostname: String = getString
    val address: String = getString
    val port: Short = messageBuffer.getShort
    val groupID: Short = messageBuffer.getShort

    new WorkerInfo(workerID, hostname, address, port, groupID)
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

  @throws(classOf[InconsistentDatatypeException])
  def readByte: Byte = {

    val code = messageBuffer.get

    if (code != Datatype.Byte.value) {
      messageBuffer.asInstanceOf[Buffer].position(messageBuffer.asInstanceOf[Buffer].position - 1)
      val message: String = s"Actual datatype ${Datatype.withValue(code).label} does not match expected datatype ${Datatype.Byte.label}"
      throw new InconsistentDatatypeException(message)
    }

    messageBuffer.get
  }

  @throws(classOf[InconsistentDatatypeException])
  def readChar: Char = {

    val code = messageBuffer.get

    if (code != Datatype.Char.value) {
      messageBuffer.asInstanceOf[Buffer].position(messageBuffer.asInstanceOf[Buffer].position - 1)
      val message: String = s"Actual datatype ${Datatype.withValue(code).label} does not match expected datatype ${Datatype.Char.label}"
      throw new InconsistentDatatypeException(message)
    }

    messageBuffer.get.asInstanceOf[Char]
  }

  @throws(classOf[InconsistentDatatypeException])
  def readShort: Short = {

    val code = messageBuffer.get

    if (code != Datatype.Short.value) {
      messageBuffer.asInstanceOf[Buffer].position(messageBuffer.asInstanceOf[Buffer].position - 1)
      val message: String = s"Actual datatype ${Datatype.withValue(code).label} does not match expected datatype ${Datatype.Short.label}"
      throw new InconsistentDatatypeException(message)
    }

    messageBuffer.getShort
  }

  @throws(classOf[InconsistentDatatypeException])
  def readInt: Int = {

    val code = messageBuffer.get

    if (code != Datatype.Int.value) {
      messageBuffer.asInstanceOf[Buffer].position(messageBuffer.asInstanceOf[Buffer].position - 1)
      val message: String = s"Actual datatype ${Datatype.withValue(code).label} does not match expected datatype ${Datatype.Int.label}"
      throw new InconsistentDatatypeException(message)
    }

    messageBuffer.getInt
  }

  @throws(classOf[InconsistentDatatypeException])
  def readLong: Long = {

    val code = messageBuffer.get

    if (code != Datatype.Long.value) {
      messageBuffer.asInstanceOf[Buffer].position(messageBuffer.asInstanceOf[Buffer].position - 1)
      val message: String = s"Actual datatype ${Datatype.withValue(code).label} does not match expected datatype ${Datatype.Long.label}"
      throw new InconsistentDatatypeException(message)
    }

    messageBuffer.getLong
  }

  @throws(classOf[InconsistentDatatypeException])
  def readFloat: Float = {

    val code = messageBuffer.get

    if (code != Datatype.Float.value) {
      messageBuffer.asInstanceOf[Buffer].position(messageBuffer.asInstanceOf[Buffer].position - 1)
      val message: String = s"Actual datatype ${Datatype.withValue(code).label} does not match expected datatype ${Datatype.Float.label}"
      throw new InconsistentDatatypeException(message)
    }

    messageBuffer.getFloat
  }

  @throws(classOf[InconsistentDatatypeException])
  def readDouble: Double = {

    val code = messageBuffer.get

    if (code != Datatype.Double.value) {
      messageBuffer.asInstanceOf[Buffer].position(messageBuffer.asInstanceOf[Buffer].position - 1)
      val message: String = s"Actual datatype ${Datatype.withValue(code).label} does not match expected datatype ${Datatype.Double.label}"
      throw new InconsistentDatatypeException(message)
    }

    messageBuffer.getDouble
  }

  @throws(classOf[InconsistentDatatypeException])
  def readString: String = {

    val code = messageBuffer.get

    if (code != Datatype.String.value) {
      messageBuffer.asInstanceOf[Buffer].position(messageBuffer.asInstanceOf[Buffer].position - 1)
      val message: String = s"Actual datatype ${Datatype.withValue(code).label} does not match expected datatype ${Datatype.String.label}"
      throw new InconsistentDatatypeException(message)
    }

    getString
  }

  @throws(classOf[InconsistentDatatypeException])
  def readParameter: Unit = {

    val code = messageBuffer.get

    if (code != Datatype.Parameter.value) {
      messageBuffer.asInstanceOf[Buffer].position(messageBuffer.asInstanceOf[Buffer].position - 1)
      val message: String = s"Actual datatype ${Datatype.withValue(code).label} does not match expected datatype ${Datatype.Parameter.label}"
      throw new InconsistentDatatypeException(message)
    }
  }

  @throws(classOf[InconsistentDatatypeException])
  def readLibraryID: Byte = {

    val code = messageBuffer.get

    if (code != Datatype.LibraryID.value) {
      messageBuffer.asInstanceOf[Buffer].position(messageBuffer.asInstanceOf[Buffer].position - 1)
      val message: String = s"Actual datatype ${Datatype.withValue(code).label} does not match expected datatype ${Datatype.LibraryID.label}"
      throw new InconsistentDatatypeException(message)
    }

    messageBuffer.get
  }

  @throws(classOf[InconsistentDatatypeException])
  def readArrayID: Short = {

    val code = messageBuffer.get

    if (code != Datatype.ArrayID.value) {
      messageBuffer.asInstanceOf[Buffer].position(messageBuffer.asInstanceOf[Buffer].position - 1)
      val message: String = s"Actual datatype ${Datatype.withValue(code).label} does not match expected datatype ${Datatype.ArrayID.label}"
      throw new InconsistentDatatypeException(message)
    }

    getArrayID
  }

  @throws(classOf[InconsistentDatatypeException])
  def readWorkerID: Short = {

    val code = messageBuffer.get

    if (code != Datatype.WorkerID.value) {
      messageBuffer.asInstanceOf[Buffer].position(messageBuffer.asInstanceOf[Buffer].position - 1)
      val message: String = s"Actual datatype ${Datatype.withValue(code).label} does not match expected datatype ${Datatype.WorkerID.label}"
      throw new InconsistentDatatypeException(message)
    }

    getWorkerID
  }

  @throws(classOf[InconsistentDatatypeException])
  def readWorkerInfo: WorkerInfo = {

    val code = messageBuffer.get

    if (code != Datatype.WorkerInfo.value) {
      messageBuffer.asInstanceOf[Buffer].position(messageBuffer.asInstanceOf[Buffer].position - 1)
      val message: String = s"Actual datatype ${Datatype.withValue(code).label} does not match expected datatype ${Datatype.WorkerInfo.label}"
      throw new InconsistentDatatypeException(message)
    }

    getWorkerInfo
  }

  @throws(classOf[InconsistentDatatypeException])
  def readArrayInfo: ArrayHandle = {

    val code = messageBuffer.get

    if (code != Datatype.ArrayInfo.value) {
      messageBuffer.asInstanceOf[Buffer].position(messageBuffer.asInstanceOf[Buffer].position - 1)
      val message: String = s"Actual datatype ${Datatype.withValue(code).label} does not match expected datatype ${Datatype.ArrayInfo.label}"
      throw new InconsistentDatatypeException(message)
    }

    getArrayInfo
  }

  @throws(classOf[InconsistentDatatypeException])
  def readArrayBlockFloat: ArrayBlockFloat = {

    val code = messageBuffer.get

    if (code != Datatype.ArrayBlockFloat.value) {
      messageBuffer.asInstanceOf[Buffer].position(messageBuffer.asInstanceOf[Buffer].position - 1)
      val message: String = s"Actual datatype ${Datatype.withValue(code).label} does not match expected datatype ${Datatype.ArrayBlockFloat.label}"
      throw new InconsistentDatatypeException(message)
    }

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

  @throws(classOf[InconsistentDatatypeException])
  def readArrayBlockDouble: ArrayBlockDouble = {

    val code = messageBuffer.get

    if (code != Datatype.ArrayBlockDouble.value) {
      messageBuffer.asInstanceOf[Buffer].position(messageBuffer.asInstanceOf[Buffer].position - 1)
      val message: String = s"Actual datatype ${Datatype.withValue(code).label} does not match expected datatype ${Datatype.ArrayBlockDouble.label}"
      throw new InconsistentDatatypeException(message)
    }

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

  def writeArrayID(value: ArrayID): this.type = {
    messageBuffer.put(Datatype.ArrayID.value)
                 .putShort(value.value)

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

  def writeParameter: this.type = {
    messageBuffer.put(Datatype.Parameter.value)

    this
  }

  // ========================================================================================

  def updateBodyLength: this.type = {
    bodyLength = messageBuffer.asInstanceOf[Buffer].position - headerLength
    messageBuffer.putInt(6, bodyLength)

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
      val currentDatatype = previewNextDatatype

      var data: String = s"$space "

      if (currentDatatype == Datatype.Parameter.value) {
        readParameter
        data = data.concat(f"${Datatype.Parameter.label}%-20s        $readString\n")

        val parameterDatatype = previewNextDatatype

        data = data.concat(f"$space     ${Datatype.withValue(parameterDatatype).label}%-16s      ")

        parameterDatatype match {
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
          case Datatype.WorkerInfo.value => data = data.concat(s" ${readWorkerInfo.toString(true)}")
          case Datatype.ArrayID.value => data = data.concat(s" ${readArrayID} ")
          case Datatype.ArrayInfo.value => data = data.concat(s" ${readArrayInfo.toString}")
          case Datatype.ArrayBlockFloat.value => data = data.concat(s" ${readArrayBlockFloat.toString(space + "                            ")}")
          case Datatype.ArrayBlockDouble.value => data = data.concat(s" ${readArrayBlockDouble.toString(space + "                            ")}")
          case Datatype.Parameter.value => data = data.concat(s" ${readParameter}")
          case _ => println("Unknown type")
        }
      }
      else {

        data = data.concat(f"${Datatype.withValue(currentDatatype).label}%-20s      ")

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
          case Datatype.WorkerInfo.value => data = data.concat(s" ${readWorkerInfo.toString(true)}")
          case Datatype.ArrayID.value => data = data.concat(s" ${readArrayID} ")
          case Datatype.ArrayInfo.value => data = data.concat(s" ${readArrayInfo.toString}")
          case Datatype.ArrayBlockFloat.value => data = data.concat(s" ${readArrayBlockFloat.toString(space + "                            ")}")
          case Datatype.ArrayBlockDouble.value => data = data.concat(s" ${readArrayBlockDouble.toString(space + "                            ")}")
          case Datatype.Parameter.value => data = data.concat(s" ${readParameter}")
          case _ => println("Unknown type")
        }
      }

      System.out.println(s"$data")
    }

    messageBuffer.asInstanceOf[Buffer].position(headerLength)

    System.out.println(s"$space ==================================================================")

    this
  }
}
