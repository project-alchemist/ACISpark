package alchemist

import scala.collection.Map

class DriverClient extends Client {          // Connects to the Alchemist driver

  def runTask(lib: LibraryHandle, methodName: String, inArgs: Parameters): Parameters = {

    writeMessage.start(clientID, sessionID, Command.RunTask)
    writeMessage.writeLibraryID(lib.id)
    writeMessage.writeString(methodName)
    inArgs.getParameters.foreach { case (name, value) => serializeParameter(name, value) }

    sendMessage

    deserializeParameters
  }

  def deserializeParameters: Parameters = {
    val outArgs: Parameters = new Parameters
    while (!readMessage.eom) {
      var p = readMessage.readParameter

      p match {
        case Parameter(n: String, v: Byte) => outArgs.add(p.asInstanceOf[Parameter[Byte]])
        case Parameter(n: String, v: Short) => outArgs.add(p.asInstanceOf[Parameter[Short]])
        case Parameter(n: String, v: Int) => outArgs.add(p.asInstanceOf[Parameter[Int]])
        case Parameter(n: String, v: Long) => outArgs.add(p.asInstanceOf[Parameter[Long]])
        case Parameter(n: String, v: Float) => outArgs.add(p.asInstanceOf[Parameter[Float]])
        case Parameter(n: String, v: Double) => outArgs.add(p.asInstanceOf[Parameter[Double]])
        case Parameter(n: String, v: Char) => outArgs.add(p.asInstanceOf[Parameter[Char]])
        case Parameter(n: String, v: String) => outArgs.add(p.asInstanceOf[Parameter[String]])
        case Parameter(n: String, v: ArrayID) => outArgs.add(p.asInstanceOf[Parameter[ArrayID]])
        case _ => "UNKNOWN TYPE"
      }
    }

    outArgs.add[Int]("rank", 32)

    outArgs
  }

  def serializeParameter(name: String, p: ParameterValue): Unit = {

    writeMessage.writeParameter.writeString(name)
    p match {
      case Parameter(n: String, v: Byte) => writeMessage.writeByte(p.asInstanceOf[Parameter[Byte]].value)
      case Parameter(n: String, v: Short) => writeMessage.writeShort(p.asInstanceOf[Parameter[Short]].value)
      case Parameter(n: String, v: Int) => writeMessage.writeInt(p.asInstanceOf[Parameter[Int]].value)
      case Parameter(n: String, v: Long) => writeMessage.writeLong(p.asInstanceOf[Parameter[Long]].value)
      case Parameter(n: String, v: Float) => writeMessage.writeFloat(p.asInstanceOf[Parameter[Float]].value)
      case Parameter(n: String, v: Double) => writeMessage.writeDouble(p.asInstanceOf[Parameter[Double]].value)
      case Parameter(n: String, v: Char) => writeMessage.writeChar(p.asInstanceOf[Parameter[Char]].value)
      case Parameter(n: String, v: String) => writeMessage.writeString(p.asInstanceOf[Parameter[String]].value)
      case Parameter(n: String, v: ArrayID) => writeMessage.writeArrayID(p.asInstanceOf[Parameter[ArrayID]].value)
      case _ => println("Unknown type")
    }
  }

//  def deserializeParameter: Parameter = {
//
//    writeMessage.writeParameter.writeString(p.name)
//    p.datatype.value match {
//      case Datatype.Byte.value => writeMessage.writeByte(p.value)
//      case Datatype.Short.value => writeMessage.writeShort(p.value)
//      case Datatype.Int.value => writeMessage.writeInt(p.value)
//      case Datatype.Long.value => writeMessage.writeLong(p.value)
//      case Datatype.Float.value => writeMessage.writeFloat(p.value)
//      case Datatype.Double.value => writeMessage.writeDouble(p.value)
//      case Datatype.Char.value => writeMessage.writeChar(p.value)
//      case Datatype.String.value => writeMessage.writeString(p.value)
//      case Datatype.ArrayID.value => writeMessage.writeArrayID(p.value)
//    }
//  }



  def requestWorkers(numWorkers: Short): Map[Short, WorkerInfo] = {

    writeMessage.start(clientID, sessionID, Command.RequestWorkers)
                .writeShort(numWorkers)

    sendMessage

    val numAssignedWorkers: Short = readMessage.readShort

    var workers: Map[Short, WorkerInfo] = Map.empty[Short, WorkerInfo]

    0 until numAssignedWorkers foreach(_ => {
      val w: WorkerInfo = readMessage.readWorkerInfo
      workers += (w.ID -> w)
    })

    workers
  }

  def yieldWorkers(yieldedWorkers: List[Byte] = List.empty[Byte]): List[Byte] = {

    println(s"Yielding Alchemist workers")

    writeMessage.start(clientID, sessionID, Command.YieldWorkers)

    sendMessage

    val message: String = readMessage.readString

    yieldedWorkers
  }

  def sendArrayInfo(name: String, numRows: Long, numCols: Long, sparse: Byte = 0, layout: Byte = 0): ArrayHandle = {

    writeMessage.start(clientID, sessionID, Command.ArrayInfo)
      .writeString(name)
      .writeLong(numRows)
      .writeLong(numCols)
      .writeByte(sparse)
      .writeByte(layout)

    sendMessage

    readMessage.readArrayInfo
  }

  def extractLayout: Array[Short] = {

    val numRows: Long = readMessage.readLong

    (0l until numRows).map(_ => readMessage.readShort).toArray
  }

  def sendAssignedWorkersInfo(preamble: String): this.type = {

    writeMessage.start(clientID, sessionID, Command.SendAssignedWorkersInfo)

    sendMessage
  }

  def readWorkerList: Array[WorkerInfo] = {

    var workerList: Array[WorkerInfo] = Array.empty[WorkerInfo]

    val numWorkers = readMessage.readShort.toInt
    0 until numWorkers foreach { _ => workerList = workerList :+ readMessage.readWorkerInfo }

    workerList
  }

  def listAllWorkers: Array[WorkerInfo] = {

    writeMessage.start(clientID, sessionID, Command.ListAllWorkers)

    sendMessage

    readWorkerList
  }

  def listActiveWorkers: Array[WorkerInfo] = {

    writeMessage.start(clientID, sessionID, Command.ListActiveWorkers)

    sendMessage

    readWorkerList
  }

  def listInactiveWorkers: Array[WorkerInfo] = {

    writeMessage.start(clientID, sessionID, Command.ListInactiveWorkers)

    sendMessage

    readWorkerList
  }

  def listAssignedWorkers: Array[WorkerInfo] = {

    writeMessage.start(clientID, sessionID, Command.ListAssignedWorkers)

    sendMessage

    readWorkerList
  }

  def loadLibrary(name: String, path: String): LibraryHandle = {

    writeMessage.start(clientID, sessionID, Command.LoadLibrary)
                .writeString(name)
                .writeString(path)

    sendMessage

    LibraryHandle(readMessage.readLibraryID, name, path)
  }

  def runTask(preamble: String): this.type = {

    writeMessage.start(clientID, sessionID, Command.RunTask)

    sendMessage
  }

  def unloadLibrary(preamble: String): this.type = {

    writeMessage.start(clientID, sessionID, Command.UnloadLibrary)

    sendMessage
  }

  def matrixInfo: this.type = {

    writeMessage.start(clientID, sessionID, Command.ArrayInfo)

    sendMessage
  }

  def matrixLayout: this.type = {

    writeMessage.start(clientID, sessionID, Command.ArrayLayout)

    sendMessage
  }

  def matrixBlock: this.type = {

    writeMessage.start(clientID, sessionID, Command.RequestArrayBlocks) // TODO: Not sure which one this is.

    sendMessage
  }

  def close: this.type = {
    yieldWorkers()
    disconnectFromAlchemist
  }

}
