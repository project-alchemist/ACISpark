package alchemist

import scala.collection.Map

class DriverClient extends Client { // Connects to the Alchemist driver

  def runTask(lib: LibraryHandle, methodName: String, inArgs: Parameters): (Parameters, Overhead, Overhead) = {

    val sendStartTime = System.nanoTime

    writeMessage.start(clientID, sessionID, Command.RunTask)
    writeMessage.writeLibraryID(lib.id)
    writeMessage.writeString(methodName)
    inArgs.getParameters.foreach { case (_, p) => writeMessage.writeParameter(p) }

    val (sendBytes, sendTime) = sendMessage
    val sendOverhead          = new Overhead(0, sendBytes, sendTime, System.nanoTime - sendStartTime)

    val (receiveBytes, receiveTime) = receiveMessage
    val receiveStartTime            = System.nanoTime

    val outArgs: Parameters = new Parameters

    while (!readMessage.eom) outArgs.add(readMessage.readParameter)

    val receiveOverhead = new Overhead(1, receiveBytes, receiveTime, System.nanoTime - receiveStartTime)

    (outArgs, sendOverhead, receiveOverhead)
  }

  def requestWorkers(numWorkers: Short): (Map[Short, WorkerInfo], Overhead, Overhead) = {

    writeMessage
      .start(clientID, sessionID, Command.RequestWorkers)
      .writeShort(numWorkers)

    val sendStartTime = System.nanoTime

    val (sendBytes, sendTime) = sendMessage
    val sendOverhead          = new Overhead(0, sendBytes, sendTime, System.nanoTime - sendStartTime)

    val (receiveBytes, receiveTime) = receiveMessage
    val receiveStartTime            = System.nanoTime

    val numAssignedWorkers: Short = readMessage.readShort

    var workers: Map[Short, WorkerInfo] = Map.empty[Short, WorkerInfo]

    0 until numAssignedWorkers foreach (_ => {
      val w: WorkerInfo = readMessage.readWorkerInfo
      workers += (w.ID -> w)
    })

    val receiveOverhead = new Overhead(1, receiveBytes, receiveTime, System.nanoTime - receiveStartTime)

    (workers, sendOverhead, receiveOverhead)
  }

  def yieldWorkers(yieldedWorkers: List[Byte] = List.empty[Byte]): (List[Byte], Overhead, Overhead) = {

    val sendStartTime = System.nanoTime

    writeMessage.start(clientID, sessionID, Command.YieldWorkers)

    val (sendBytes, sendTime) = sendMessage
    val sendOverhead          = new Overhead(0, sendBytes, sendTime, System.nanoTime - sendStartTime)

    val (receiveBytes, receiveTime) = receiveMessage
    val receiveStartTime            = System.nanoTime

    val receiveOverhead = new Overhead(1, receiveBytes, receiveTime, System.nanoTime - receiveStartTime)

    (yieldedWorkers, sendOverhead, receiveOverhead)
  }

  def sendMatrixInfo(
    name: String,
    numRows: Long,
    numCols: Long,
    sparse: Byte = 0,
    layout: Layout = Layout.MC_MR
  ): (MatrixHandle, Overhead, Overhead) = {

    val sendStartTime = System.nanoTime

    writeMessage
      .start(clientID, sessionID, Command.MatrixInfo)
      .writeString(name)
      .writeLong(numRows)
      .writeLong(numCols)
      .writeByte(sparse)
      .writeByte(layout.value)

    val (sendBytes, sendTime) = sendMessage
    val sendOverhead          = new Overhead(0, sendBytes, sendTime, System.nanoTime - sendStartTime)

    val (receiveBytes, receiveTime) = receiveMessage
    val receiveStartTime            = System.nanoTime

    val mh: MatrixHandle = readMessage.readMatrixInfo

    val receiveOverhead = new Overhead(1, receiveBytes, receiveTime, System.nanoTime - receiveStartTime)

    (mh, sendOverhead, receiveOverhead)
  }

  def readWorkerList: Array[WorkerInfo] = {

    var workerList: Array[WorkerInfo] = Array.empty[WorkerInfo]

    val numWorkers = readMessage.readShort.toInt
    0 until numWorkers foreach { _ =>
      workerList = workerList :+ readMessage.readWorkerInfo
    }

    workerList
  }

  def listAllWorkers: (Array[WorkerInfo], Overhead, Overhead) = {

    val sendStartTime = System.nanoTime

    writeMessage.start(clientID, sessionID, Command.ListAllWorkers)

    val (sendBytes, sendTime) = sendMessage
    val sendOverhead          = new Overhead(0, sendBytes, sendTime, System.nanoTime - sendStartTime)

    val (receiveBytes, receiveTime) = receiveMessage
    val receiveStartTime            = System.nanoTime

    val workerList: Array[WorkerInfo] = readWorkerList

    val receiveOverhead = new Overhead(1, receiveBytes, receiveTime, System.nanoTime - receiveStartTime)

    (workerList, sendOverhead, receiveOverhead)
  }

  def listActiveWorkers: (Array[WorkerInfo], Overhead, Overhead) = {

    val sendStartTime = System.nanoTime

    writeMessage.start(clientID, sessionID, Command.ListActiveWorkers)

    val (sendBytes, sendTime) = sendMessage
    val sendOverhead          = new Overhead(0, sendBytes, sendTime, System.nanoTime - sendStartTime)

    val (receiveBytes, receiveTime) = receiveMessage
    val receiveStartTime            = System.nanoTime

    val workerList: Array[WorkerInfo] = readWorkerList

    val receiveOverhead = new Overhead(1, receiveBytes, receiveTime, System.nanoTime - receiveStartTime)

    (workerList, sendOverhead, receiveOverhead)
  }

  def listInactiveWorkers: (Array[WorkerInfo], Overhead, Overhead) = {

    val sendStartTime = System.nanoTime

    writeMessage.start(clientID, sessionID, Command.ListInactiveWorkers)

    val (sendBytes, sendTime) = sendMessage
    val sendOverhead          = new Overhead(0, sendBytes, sendTime, System.nanoTime - sendStartTime)

    val (receiveBytes, receiveTime) = receiveMessage
    val receiveStartTime            = System.nanoTime

    val workerList: Array[WorkerInfo] = readWorkerList

    val receiveOverhead = new Overhead(1, receiveBytes, receiveTime, System.nanoTime - receiveStartTime)

    (workerList, sendOverhead, receiveOverhead)
  }

  def listAssignedWorkers: (Array[WorkerInfo], Overhead, Overhead) = {

    val sendStartTime = System.nanoTime

    writeMessage.start(clientID, sessionID, Command.ListAssignedWorkers)

    val (sendBytes, sendTime) = sendMessage
    val sendOverhead          = new Overhead(0, sendBytes, sendTime, System.nanoTime - sendStartTime)

    val (receiveBytes, receiveTime) = receiveMessage
    val receiveStartTime            = System.nanoTime

    val workerList: Array[WorkerInfo] = readWorkerList

    val receiveOverhead = new Overhead(1, receiveBytes, receiveTime, System.nanoTime - receiveStartTime)

    (workerList, sendOverhead, receiveOverhead)
  }

  def loadLibrary(name: String, path: String): (LibraryHandle, Overhead, Overhead) = {

    writeMessage
      .start(clientID, sessionID, Command.LoadLibrary)
      .writeString(name)
      .writeString(path)

    val sendStartTime = System.nanoTime

    val (sendBytes, sendTime) = sendMessage
    val sendOverhead          = new Overhead(0, sendBytes, sendTime, System.nanoTime - sendStartTime)

    val (receiveBytes, receiveTime) = receiveMessage
    val receiveStartTime            = System.nanoTime

    val lh: LibraryHandle = LibraryHandle(readMessage.readLibraryID, name, path)

    val receiveOverhead = new Overhead(1, receiveBytes, receiveTime, System.nanoTime - receiveStartTime)

    (lh, sendOverhead, receiveOverhead)
  }

  def unloadLibrary(libID: LibraryID): (Overhead, Overhead) = {

    writeMessage
      .start(clientID, sessionID, Command.UnloadLibrary)
      .writeLibraryID(libID)

    val sendStartTime = System.nanoTime

    val (sendBytes, sendTime) = sendMessage
    val sendOverhead          = new Overhead(0, sendBytes, sendTime, System.nanoTime - sendStartTime)

    val (receiveBytes, receiveTime) = receiveMessage
    val receiveStartTime            = System.nanoTime

    val receiveOverhead = new Overhead(1, receiveBytes, receiveTime, System.nanoTime - receiveStartTime)

    (sendOverhead, receiveOverhead)
  }
}
