package alchemist

import scala.collection.Map
import scala.collection.immutable.Seq
import scala.util.Sorting
import java.io.{BufferedReader, FileInputStream, InputStreamReader, DataInputStream => JDataInputStream, DataOutputStream => JDataOutputStream}

import alchemist.AlchemistSession.{checkIfConnected, printWorkers}
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix

// spark-core
import org.apache.spark.rdd.RDD
// spark-sql
import org.apache.spark.sql.SparkSession
// spark-mllib
import org.apache.spark.mllib.linalg.distributed.{DistributedMatrix, IndexedRow, IndexedRowMatrix, RowMatrix}


case class MatrixID(value: Short) extends Serializable
case class LibraryID(value: Byte) extends Serializable


object AlchemistSession {

  var verbose = true
  var showOverheads = false

  if (verbose) println("Starting Alchemist session")

  var connected: Boolean = false

  val driver: DriverClient = new DriverClient()
  var workers: Map[Short, WorkerInfo] = Map.empty[Short, WorkerInfo]
  var bufferLength: Int = 10000000

  var libraries: Map[Byte, LibraryHandle] = Map.empty[Byte, LibraryHandle]

  var spark: SparkSession = _

  if (verbose) println("Alchemist session ready")

  def main(args: Array[String]): Unit = { }

  def initialize(_spark: SparkSession,
                 _bufferLength: Int = 10000000,
                 _verbose: Boolean = true,
                 _showOverheads: Boolean = false): this.type = {

    spark = _spark
    bufferLength = _bufferLength
    verbose = _verbose
    showOverheads = _showOverheads

    driver.setBufferLength(bufferLength)

    this
  }

  // ------------------------------------------- Connection -------------------------------------------

  def connect(_hostname: String = "", _port: Int = 0): this.type = {

    var hostname = _hostname
    var port = _port
    if (hostname == "" || port == 0) {
      if (verbose) println(s"Reading Alchemist address and port from file")
      try {
        val fstream: FileInputStream = new FileInputStream("connection.info")
        // Get the object of DataInputStream
        val in: JDataInputStream = new JDataInputStream(fstream)
        val br: BufferedReader = new BufferedReader(new InputStreamReader(in))
        hostname = br.readLine()
        port = Integer.parseInt(br.readLine)

        in.close()                    // Close the input stream
      }
      catch {
        case e: Exception => println("Got exception: " + e.getMessage)
      }
    }
    if (hostname == "0.0.0.0")
      hostname = "localhost"

    if (verbose) println(s"Connecting to Alchemist at $hostname:$port")
    try {
      connected = driver.connect(hostname, port)
    }
    catch {
      case e: InvalidHandshakeException => println(e.getMessage)
    }

    this
  }

  def checkIfConnected: Boolean = {

    if (connected) true
    else {
      println("ERROR: Not connected to Alchemist")
      false
    }
  }

  def sendTestString(_testString: String = ""): this.type = {

    if (checkIfConnected) {
      val testString: String = {
        if (_testString.isEmpty)
          "This is a test string from a Spark application"
        else _testString
      }

      if (verbose) println(s"Sending the following test string to Alchemist: $testString")
      val (responseString, sendOverhead, receiveOverhead): (String, Overhead, Overhead) = driver.sendTestString(testString)
      if (verbose) println(s"Received the following response string from Alchemist: $responseString")

      if (verbose && showOverheads) printOverheads("getMatrixHandle", sendOverhead, receiveOverhead)
    }

    this
  }

  def disconnect: this.type = {

    driver.close

    this
  }

  def stop: this.type = {

    if (verbose) println("Ending Alchemist session")
    disconnect
  }

  def setVerbose(_verbose: Boolean = true): this.type = {
    verbose = _verbose

    this
  }

  def setPrintOverheads(_showOverheads: Boolean = true): this.type = {
    showOverheads = _showOverheads

    this
  }

  def requestTestString: this.type = {
    if (verbose) {
      println("Requesting test string from Alchemist")
      println(s"Received test string '${driver.requestTestString._1}'")
    }

    this
  }

  def requestWorkers(numWorkers: Short): this.type = {

    if (checkIfConnected) {
      if (numWorkers < 1)
        println(s"Cannot request $numWorkers Alchemist workers")
      else {
        if (verbose) println(s"Requesting $numWorkers Alchemist workers")

        val (_workers, sendOverhead, receiveOverhead) = driver.requestWorkers(numWorkers)

        workers = _workers

        if (verbose && showOverheads) printOverheads("requestWorkers", sendOverhead, receiveOverhead)

        if (verbose) {
          if (workers.isEmpty)
            println(s"Alchemist could not assign $numWorkers workers")
          else {
            println(s"Assigned $numWorkers workers:")
            workers.foreach(w => println(s"    ${w.toString}"))
          }
        }
      }
    }

    this
  }

  // ----------------------------------------- Library Management -----------------------------------------


  def loadLibrary(name: String, path: String, jar: String): LibraryHandle = {

    if (checkIfConnected) {
      if (verbose) println(s"Loading library $name at $path")
      try {
        val (lh, sendOverhead, receiveOverhead): (LibraryHandle, Overhead, Overhead) = driver.loadLibrary(name, path)
        libraries = libraries + (lh.id.value -> lh)

        if (verbose) println(s"Loaded library $name with ID ${lh.id.value}")

        lh
      }
      catch {
        case e: LibraryNotFoundException => {
          println(e.getMessage)
          new LibraryHandle
        }
      }
    }
    else new LibraryHandle
  }

  // --------------------------------------------------------------------------------------------------------

  def runTask(lib: LibraryHandle, name: String, inArgs: Parameters): Parameters = {

    if (checkIfConnected) {
      if (verbose) print(s"Alchemist running task '$name' ... ")
      val (outArgs, sendOverhead, receiveOverhead): (Parameters, Overhead, Overhead) = driver.runTask(lib, name, inArgs)
      if (verbose) println("done")

      if (verbose && showOverheads) printOverheads("runTask", sendOverhead, receiveOverhead)

      outArgs
    }
    else new Parameters
  }

  def getMatrixHandle(mat: DistributedMatrix, name: String = ""): MatrixHandle = {

    if (checkIfConnected) {
      val (mh, sendOverhead, receiveOverhead): (MatrixHandle, Overhead, Overhead) = driver.sendMatrixInfo(name, mat.numRows, mat.numCols)

      if (verbose && showOverheads) printOverheads("getMatrixHandle", sendOverhead, receiveOverhead)

      mh
    }
    else new MatrixHandle
  }

  // --------------------------------------- IndexedRowMatrices ---------------------------------------

  def sendIndexedRowMatrix(mat: IndexedRowMatrix): MatrixHandle = {

    if (checkIfConnected) {
      val mh: MatrixHandle = getMatrixHandle(mat)

      mat.rows.mapPartitionsWithIndex { (idx, part) =>

        val wc: WorkerClient = new WorkerClient
        val indexedRows: Array[IndexedRow] = part.toArray
        var sendOverheads: Array[Array[Overhead]] = Array.empty[Array[Overhead]]
        var receiveOverheads: Array[Array[Overhead]] = Array.empty[Array[Overhead]]

        if (verbose) Thread.sleep(idx * 100)

        workers foreach (w => {
          if (verbose) println(s"Spark executor ${idx}: Connecting to Alchemist worker ${w._2.ID} at ${w._2.address}:${w._2.port}")
          if (wc.connect(w._2.ID, w._2.hostname, w._2.address, w._2.port)) {
            if (verbose) println(s"Spark executor ${idx}: Connected to Alchemist worker ${w._2.ID} at ${w._2.address}:${w._2.port}, sending data ...")
            val (workerSendOverheads, workerReceiveOverheads) = wc.sendIndexedRows(mh, indexedRows, idx)
            sendOverheads = sendOverheads :+ workerSendOverheads
            receiveOverheads = receiveOverheads :+ workerReceiveOverheads
          }
        })
        if (verbose) println(s"Spark executor ${idx}: Finished sending data")

        part
      }.count

      mh
    }
    else new MatrixHandle
  }

  def getIndexedRowMatrix(mh: MatrixHandle): IndexedRowMatrix = {

    if (checkIfConnected) {
      val rowIndices: RDD[Long] = spark.sparkContext.parallelize(0l until mh.numRows)
      val times: Array[Long] = Array.fill[Long](4)(0)

      val mat: IndexedRowMatrix = new IndexedRowMatrix(
        spark.sparkContext.union(
          rowIndices.mapPartitionsWithIndex { (idx, part) => {

            val wc: WorkerClient = new WorkerClient
            wc.setBufferLength(bufferLength)
            val rowIndicesArray: Array[Long] = part.toArray
            var requestedIndexedRows: Array[IndexedRow] = Array.empty[IndexedRow]

            var sendOverheads: Array[Array[Overhead]] = Array.empty[Array[Overhead]]
            var receiveOverheads: Array[Array[Overhead]] = Array.empty[Array[Overhead]]

            var tempRows: scala.collection.mutable.Map[Int, Array[Double]] = scala.collection.mutable.Map.empty[Int, Array[Double]]
            rowIndicesArray foreach (r => tempRows += (r.toInt -> Array.fill[Double](mh.numCols.toInt)(0.0)))

            workers foreach (w => {
              if (verbose) println(s"Spark executor ${idx}: Connecting to Alchemist worker ${w._2.ID} at ${w._2.address}:${w._2.port}")
              if (wc.connect(w._2.ID, w._2.hostname, w._2.address, w._2.port)) {
                if (verbose) println(s"Spark executor ${idx}: Connected to Alchemist worker ${w._2.ID} at ${w._2.address}:${w._2.port}, requesting data ...")
                val (_tempRows, workerSendOverheads, workerReceiveOverheads) = wc.getIndexedRows(mh, rowIndicesArray, tempRows, idx)
                tempRows = _tempRows
                sendOverheads = sendOverheads :+ workerSendOverheads
                receiveOverheads = receiveOverheads :+ workerReceiveOverheads
              }
            })

            rowIndicesArray foreach (r => {
              requestedIndexedRows = requestedIndexedRows :+ new IndexedRow(r.toInt, new DenseVector(tempRows(r.toInt)))
            })

            if (verbose) println(s"Spark executor ${idx}: Finished receiving data")
            requestedIndexedRows.iterator
          }
          }))

      mat
    }
    else new IndexedRowMatrix(spark.sparkContext.parallelize((0L to 2 - 1)
      .map(x => IndexedRow(x, new DenseVector(Array.fill(2)(0.0))))))
  }

  def printIndexedRowMatrix(mat: IndexedRowMatrix): this.type = {

    mat.rows.mapPartitionsWithIndex { (idx, part) => {
      Thread.sleep(idx*100)
      val indexedRows: Array[IndexedRow] = part.toArray

      println(s"Partition $idx:")
      indexedRows.foreach(r => println(r))

      part
    }}.count

    this
  }

  // -------------------------------------------- Worker Management ----------------------------------------------

  def yieldWorkers(yieldedWorkers: List[Byte] = List.empty[Byte]): this.type = {

    if (checkIfConnected) {
      val (deallocatedWorkers, sendOverhead, receiveOverhead) = driver.yieldWorkers(yieldedWorkers)

      if (verbose) {
        if (deallocatedWorkers.isEmpty)
          println("No workers were deallocated\n")
        else {
          print("Deallocated workers ")
          deallocatedWorkers.zipWithIndex.foreach {
            case (w, i) => {
              if (i < deallocatedWorkers.length) print(s"$w, ")
              else print(s"and $w")
            }
          }
        }
      }

      if (verbose && showOverheads) printOverheads("yieldWorkers", sendOverhead, receiveOverhead)
    }

    this
  }

  def listAlchemistWorkers: this.type = {

    if (checkIfConnected) {
      val (workerList, sendOverhead, receiveOverhead) = driver.listAllWorkers
      printWorkers(workerList, "inactive ")

      if (verbose && showOverheads) printOverheads("listAlchemistWorkers", sendOverhead, receiveOverhead)
    }

    this
  }

  def listAllWorkers: this.type = {

    if (checkIfConnected) {
      val (workerList, sendOverhead, receiveOverhead) = driver.listAllWorkers
      printWorkers(workerList, "inactive ")

      if (verbose && showOverheads) printOverheads("listAllWorkers", sendOverhead, receiveOverhead)
    }

    this
  }

  def listInactiveWorkers: this.type = {

    if (checkIfConnected) {
      val (workerList, sendOverhead, receiveOverhead) = driver.listInactiveWorkers
      printWorkers(workerList, "inactive ")

      if (verbose && showOverheads) printOverheads("listInactiveWorkers", sendOverhead, receiveOverhead)
    }

    this
  }

  def listActiveWorkers: this.type = {

    if (checkIfConnected) {
      val (workerList, sendOverhead, receiveOverhead) = driver.listActiveWorkers
      printWorkers(workerList, "active ")

      if (verbose && showOverheads) printOverheads("listActiveWorkers", sendOverhead, receiveOverhead)
    }

    this
  }

  def listAssignedWorkers: this.type = {

    if (checkIfConnected) {
      val (workerList, sendOverhead, receiveOverhead) = driver.listAssignedWorkers
      printWorkers(workerList, "assigned ")

      if (verbose && showOverheads) printOverheads("listAssignedWorkers", sendOverhead, receiveOverhead)
    }

    this
  }

  def printWorkers(workerList: Array[WorkerInfo], workerType: String = ""): this.type = {

    if (verbose) {
      workerList.length match {
        case 0 => println(s"No ${workerType}workers")
        case 1 => println(s"Listing 1 ${workerType}worker")
        case _ => println(s"Listing ${workerList.length} ${workerType}workers")
      }

      workerList foreach { w => println(s"    ${w.toString(true)}") }
    }

    this
  }

  // ----------------------------------------- Utility functions -----------------------------------------

  def printOverheads(methodName: String, sendOverhead: Overhead, receiveOverhead: Overhead): this.type = {

    println(s"Overheads for method $methodName:")
    println(sendOverhead.toString("    "))
    println(receiveOverhead.toString("    "))

    this
  }
}
