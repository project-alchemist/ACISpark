package alchemist

import scala.reflect.ClassTag
import java.nio.{Buffer, ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.distributed.IndexedRow

class Message() {

  val headerLength: Int = 10
  var maxBodyLength: Int = 10000000

  var messageBuffer: ByteBuffer = ByteBuffer.allocate(headerLength + maxBodyLength).order(ByteOrder.BIG_ENDIAN)

  var clientID: Short = 0
  var sessionID: Short = 0
  var commandCode: Byte = Command.Wait.value
  var errorCode: Byte = Error.None.value
  var bodyLength: Int = 0

  def setBufferLength(bufferLength: Int = 10000000): this.type = {

    maxBodyLength = bufferLength
    messageBuffer = ByteBuffer.allocate(headerLength + maxBodyLength).order(ByteOrder.BIG_ENDIAN)
    bodyLength = 0

    this
  }

  def reset: this.type = {

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

  def getMatrixID: MatrixID = {
    MatrixID(messageBuffer.getShort)
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

  def getByteMatrix(length: Int): Array[Byte] = {
    val bb: Array[Byte] = ByteBuffer.allocate(length).order(ByteOrder.BIG_ENDIAN).array
    messageBuffer.get(bb, 0, length)
    bb
  }

  def getString: String = {
    val strLength: Short = messageBuffer.getShort
    new String(getByteMatrix(strLength.asInstanceOf[Int]), StandardCharsets.UTF_8)
  }

  def getIndexedRow: IndexedRow = {
    val index: Long = messageBuffer.getLong
    val length: Long = messageBuffer.getLong
    val values: Array[Double] = Array.fill[Double](length.toInt)(0.0)
    for (i <- 0 until length.toInt)
      values(i) = messageBuffer.getDouble

    new IndexedRow(index, new DenseVector(values))
  }

//  def getIndexedRow(row: IndexedRow): IndexedRow = {
//    row.index = messageBuffer.getLong
//    val length: Long = messageBuffer.getLong
//    val values: Matrix[Double] = Matrix.fill[Double](length.toInt)(0.0)
//    for (i <- 0 until length.toInt)
//      values(i) = messageBuffer.getDouble
//  }

  def getMatrixInfo: MatrixHandle = {
    val ID: MatrixID = getMatrixID
    val name: String = getString
    val numRows: Long = messageBuffer.getLong
    val numCols: Long = messageBuffer.getLong
    val sparse: Byte = messageBuffer.get
    val layout: Byte = messageBuffer.get
    val numGridRows: Short = messageBuffer.getShort
    val numGridCols: Short = messageBuffer.getShort
    var gridArray: Map[Short, Array[Short]] = Map.empty[Short, Array[Short]]
    (0 until numGridRows * numGridCols).foreach (_ =>
      gridArray = gridArray + (messageBuffer.getShort -> Array(messageBuffer.getShort, messageBuffer.getShort))
    )

    new MatrixHandle(ID, name, numRows, numCols, sparse, layout, new ProcessGrid(numGridRows, numGridCols, gridArray))
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
  def readParameter: ParameterValue = {

    val code = messageBuffer.get

    if (code != Datatype.Parameter.value) {
      messageBuffer.asInstanceOf[Buffer].position(messageBuffer.asInstanceOf[Buffer].position - 1)
      val message: String = s"Actual datatype ${Datatype.withValue(code).label} does not match expected datatype ${Datatype.Parameter.label}"
      throw new InconsistentDatatypeException(message)
    }

    val name: String = readString
    val parameterDatatype = previewNextDatatype

    parameterDatatype match {
      case Datatype.Byte.value => Parameter[Byte](name, readByte)
      case Datatype.Char.value => Parameter[Char](name, readChar)
      case Datatype.Short.value => Parameter[Short](name, readShort)
      case Datatype.Int.value => Parameter[Int](name, readInt)
      case Datatype.Long.value => Parameter[Long](name, readLong)
      case Datatype.Float.value => Parameter[Float](name, readFloat)
      case Datatype.Double.value => Parameter[Double](name, readDouble)
      case Datatype.String.value => Parameter[String](name, readString)
      case Datatype.MatrixID.value => Parameter[MatrixID](name, readMatrixID)
      case _ => {
        val message: String = s"Parameters cannot have datatype ${Datatype.withValue(parameterDatatype).label}"
        throw new InconsistentDatatypeException(message)
      }
    }
  }

  @throws(classOf[InconsistentDatatypeException])
  def readLibraryID: LibraryID = {

    val code = messageBuffer.get

    if (code != Datatype.LibraryID.value) {
      messageBuffer.asInstanceOf[Buffer].position(messageBuffer.asInstanceOf[Buffer].position - 1)
      val message: String = s"Actual datatype ${Datatype.withValue(code).label} does not match expected datatype ${Datatype.LibraryID.label}"
      throw new InconsistentDatatypeException(message)
    }

    LibraryID(messageBuffer.get)
  }

  @throws(classOf[InconsistentDatatypeException])
  def readMatrixID: MatrixID = {

    val code = messageBuffer.get

    if (code != Datatype.MatrixID.value) {
      messageBuffer.asInstanceOf[Buffer].position(messageBuffer.asInstanceOf[Buffer].position - 1)
      val message: String = s"Actual datatype ${Datatype.withValue(code).label} does not match expected datatype ${Datatype.MatrixID.label}"
      throw new InconsistentDatatypeException(message)
    }

    getMatrixID
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
  def readIndexedRow: IndexedRow = {

    val code = messageBuffer.get

    if (code != Datatype.IndexedRow.value) {
      messageBuffer.asInstanceOf[Buffer].position(messageBuffer.asInstanceOf[Buffer].position - 1)
      val message: String = s"Actual datatype ${Datatype.withValue(code).label} does not match expected datatype ${Datatype.IndexedRow.label}"
      throw new InconsistentDatatypeException(message)
    }

    getIndexedRow
  }

  @throws(classOf[InconsistentDatatypeException])
  def readMatrixInfo: MatrixHandle = {

    val code = messageBuffer.get

    if (code != Datatype.MatrixInfo.value) {
      messageBuffer.asInstanceOf[Buffer].position(messageBuffer.asInstanceOf[Buffer].position - 1)
      val message: String = s"Actual datatype ${Datatype.withValue(code).label} does not match expected datatype ${Datatype.MatrixInfo.label}"
      throw new InconsistentDatatypeException(message)
    }

    getMatrixInfo
  }

  @throws(classOf[InconsistentDatatypeException])
  def readMatrixBlock(readData: Boolean = true): MatrixBlock = {

    val code = messageBuffer.get

    if (code != Datatype.MatrixBlock.value) {
      messageBuffer.asInstanceOf[Buffer].position(messageBuffer.asInstanceOf[Buffer].position - 1)
      val message: String = s"Actual datatype ${Datatype.withValue(code).label} does not match expected datatype ${Datatype.MatrixBlock.label}"
      throw new InconsistentDatatypeException(message)
    }

    val rows: Array[Long] = Array(0l, 0l, 1l)
    val cols: Array[Long] = Array(0l, 0l, 1l)

    for (i <- 0 until 3)
      rows(i) = messageBuffer.getLong

    for (i <- 0 until 3)
      cols(i) = messageBuffer.getLong

    val size: Long = math.ceil(1.0 * ((rows(1) - rows(0))/ rows(2))+1).toLong * math.ceil(1.0 * ((cols(1)-cols(0))/ cols(2))+1).toLong

    if (readData) {
      val data = Array.fill[Double](size.toInt)(0.0)
      (0 until size.toInt).foreach(i => data(i) = messageBuffer.getDouble)

      new MatrixBlock(data, rows, cols)
    }
    else {
      val data = Array.empty[Double]

      new MatrixBlock(data, rows, cols)
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

  def putDatatype(value: Byte): this.type = {
    messageBuffer.put(value)
    this
  }

  def putByte(value: Byte): this.type = {
    messageBuffer.put(value)
    this
  }

  def putChar(value: Char): this.type = {
    messageBuffer.put(value.toByte)
    this
  }

  def putShort(value: Short): this.type = {
    messageBuffer.putShort(value)
    this
  }

  def putInt(value: Int): this.type = {
    messageBuffer.putInt(value)

    this
  }

  def putLong(value: Long): this.type = {
    messageBuffer.putLong(value)
    this
  }

  def putFloat(value: Float): this.type = {
    messageBuffer.putFloat(value)
    this
  }

  def putDouble(value: Double): this.type = {
    messageBuffer.putDouble(value)
    this
  }

  def putString(value: String): this.type = {
    putShort(value.length.asInstanceOf[Short])
    messageBuffer.put(value.getBytes(StandardCharsets.UTF_8).array)
    this
  }

  def putLibraryID(id: LibraryID): this.type = {
    messageBuffer.put(id.value)
    this
  }

  def putIndexedRow(index: Long, length: Long, values: Array[Double]): this.type = {
    messageBuffer.putLong(index).putLong(length)
    values foreach (v => messageBuffer.putDouble(v))
    this
  }

  def putMatrixID(id: MatrixID): this.type = {
    messageBuffer.putShort(id.value)
    this
  }

  def putMatrixInfo(value: MatrixHandle): this.type = {
    putShort(value.id.value)
      .putString(value.name)
      .putLong(value.numRows)
      .putLong(value.numCols)
      .putByte(value.sparse)
      .putByte(value.layout)
      .putShort(value.grid.numRows)
      .putShort(value.grid.numCols)

    this
  }

  def putMatrixBlock(block: MatrixBlock): this.type = {
    block.rows.foreach(r => messageBuffer.putLong(r))
    block.cols.foreach(c => messageBuffer.putLong(c))
    block.data.foreach(v => messageBuffer.putDouble(v))

    this
  }

  def putParameter(p: ParameterValue): this.type = {

    p match {
      case Parameter(n: String, v: Byte) => writeString(p.asInstanceOf[Parameter[Byte]].name).writeByte(p.asInstanceOf[Parameter[Byte]].value)
      case Parameter(n: String, v: Short) => writeString(p.asInstanceOf[Parameter[Short]].name).writeShort(p.asInstanceOf[Parameter[Short]].value)
      case Parameter(n: String, v: Int) => writeString(p.asInstanceOf[Parameter[Int]].name).writeInt(p.asInstanceOf[Parameter[Int]].value)
      case Parameter(n: String, v: Long) => writeString(p.asInstanceOf[Parameter[Long]].name).writeLong(p.asInstanceOf[Parameter[Long]].value)
      case Parameter(n: String, v: Float) => writeString(p.asInstanceOf[Parameter[Float]].name).writeFloat(p.asInstanceOf[Parameter[Float]].value)
      case Parameter(n: String, v: Double) => writeString(p.asInstanceOf[Parameter[Double]].name).writeDouble(p.asInstanceOf[Parameter[Double]].value)
      case Parameter(n: String, v: Char) => writeString(p.asInstanceOf[Parameter[Char]].name).writeChar(p.asInstanceOf[Parameter[Char]].value)
      case Parameter(n: String, v: String) => writeString(p.asInstanceOf[Parameter[String]].name).writeString(p.asInstanceOf[Parameter[String]].value)
      case Parameter(n: String, v: MatrixID) => writeString(p.asInstanceOf[Parameter[MatrixID]].name).writeMatrixID(p.asInstanceOf[Parameter[MatrixID]].value)
      case _ => writeString("UNKNOWN PARAMETER")
    }

    this
  }

  def writeByte(value: Byte): this.type = putDatatype(Datatype.Byte.value).putByte(value)

  def writeChar(value: Char): this.type = putDatatype(Datatype.Char.value).putChar(value)

  def writeShort(value: Short): this.type = putDatatype(Datatype.Short.value).putShort(value)

  def writeInt(value: Int): this.type = putDatatype(Datatype.Int.value).putInt(value)

  def writeLong(value: Long): this.type = putDatatype(Datatype.Long.value).putLong(value)

  def writeFloat(value: Float): this.type = putDatatype(Datatype.Float.value).putFloat(value)

  def writeDouble(value: Double): this.type = putDatatype(Datatype.Double.value).putDouble(value)

  def writeString(value: String): this.type = putDatatype(Datatype.String.value).putString(value)

  def writeLibraryID(value: LibraryID): this.type = putDatatype(Datatype.LibraryID.value).putLibraryID(value)

  def writeIndexedRow(index: Long, length: Long, values: Array[Double]): this.type = putDatatype(Datatype.IndexedRow.value).putIndexedRow(index, length, values)

  def writeMatrixID(id: MatrixID): this.type = putDatatype(Datatype.MatrixID.value).putMatrixID(id)

  def writeMatrixInfo(value: MatrixHandle): this.type = putDatatype(Datatype.MatrixInfo.value).putMatrixInfo(value)

  def writeMatrixBlock(block: MatrixBlock): this.type = putDatatype(Datatype.MatrixBlock.value).putMatrixBlock(block)

  def writeParameter(p: ParameterValue): this.type = putDatatype(Datatype.Parameter.value).putParameter(p)

  // ========================================================================================

  def updateBodyLength: this.type = {
    bodyLength = messageBuffer.asInstanceOf[Buffer].position - headerLength
    messageBuffer.putInt(6, bodyLength)

    this
  }

  def finish: Array[Byte] = {
    updateBodyLength.readHeader.resetPosition

    messageBuffer.array.slice(0, headerLength + bodyLength)
  }

  def resetPosition: this.type = {
    messageBuffer.asInstanceOf[Buffer].position(headerLength)

    this
  }

  // ========================================================================================

  def print: this.type = {

    val space: String = "                                              "

    readHeader
    resetPosition

    System.out.println()
    System.out.println(s"$space ==================================================================")
    System.out.println(s"$space Client ID:                      $clientID")
    System.out.println(s"$space Session ID:                     $sessionID")
    System.out.println(s"$space Command code:                   $commandCode (${Command.withValue(commandCode).label})")
    System.out.println(s"$space Error code:                     $errorCode (${Error.withValue(errorCode).label})")
    System.out.println(s"$space Message body length:            $bodyLength")
    System.out.println(s"$space ------------------------------------------------------------------")
    System.out.println(" ")

    while (!eom) {
      val currentDatatype = previewNextDatatype

      var data: String = s"$space "

      data = data.concat(f"${Datatype.withValue(currentDatatype).label}%-25s      ")

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
        case Datatype.MatrixID.value => data = data.concat(s" ${readMatrixID.value} ")
        case Datatype.MatrixInfo.value => data = data.concat(s" ${readMatrixInfo.toString(space + "                                ")}")
        case Datatype.MatrixBlock.value => data = data.concat(s" ${readMatrixBlock(true).toString(space + "                                 ")}")
        case Datatype.IndexedRow.value => data = data.concat(s" ${readIndexedRow.toString}")
        case Datatype.Parameter.value => {
          val p = readParameter

          data = data.concat(s" ${p match {
            case Parameter(n: String, v: Byte) => p.asInstanceOf[Parameter[Byte]].toString(true)
            case Parameter(n: String, v: Short) => p.asInstanceOf[Parameter[Short]].toString(true)
            case Parameter(n: String, v: Int) => p.asInstanceOf[Parameter[Int]].toString(true)
            case Parameter(n: String, v: Long) => p.asInstanceOf[Parameter[Long]].toString(true)
            case Parameter(n: String, v: Float) => p.asInstanceOf[Parameter[Float]].toString(true)
            case Parameter(n: String, v: Double) => p.asInstanceOf[Parameter[Double]].toString(true)
            case Parameter(n: String, v: Char) => p.asInstanceOf[Parameter[Char]].toString(true)
            case Parameter(n: String, v: String) => p.asInstanceOf[Parameter[String]].toString(true)
            case Parameter(n: String, v: MatrixID) => p.asInstanceOf[Parameter[MatrixID]].toString(true)
            case Parameter(n: String, v: MatrixHandle) => p.asInstanceOf[Parameter[MatrixHandle]].toString(true)
            case _ => "UNKNOWN TYPE"
          }}")
        }
        case _ => println("UNKNOWN TYPE")
      }

      System.out.println(s"$data")
    }

    resetPosition

    System.out.println(s"$space ==================================================================")

    this
  }
}
