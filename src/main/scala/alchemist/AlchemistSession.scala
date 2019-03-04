package alchemist

import scala.io.Source
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}

import scala.collection.mutable.ArrayBuffer
import scala.collection.Map
import scala.collection.immutable.Seq
import scala.util.Random
import java.lang.reflect.Method
import java.io.{BufferedReader, File, FileInputStream, InputStreamReader, DataInputStream => JDataInputStream, DataOutputStream => JDataOutputStream}

// spark-core
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
// spark-sql
import org.apache.spark.sql.SparkSession
// spark-mllib
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.distributed.{DistributedMatrix, IndexedRow, IndexedRowMatrix, RowMatrix}
import scala.math.max


case class ArrayID(value: Short)


object AlchemistSession {
  println("Starting Alchemist session")

  var connected: Boolean = false

  val driver: DriverClient = new DriverClient()
  var workers: Map[Short, WorkerInfo] = Map.empty[Short, WorkerInfo]
  var libraries: Map[Byte, LibraryHandle] = Map.empty[Byte, LibraryHandle]

  var spark: SparkSession = _

  println("Alchemist session ready")

  def main(args: Array[String]) {

//    initialize
//    connect("0.0.0.0", 24960)
//    if (connected) {
//      println("Connected to Alchemist")
//    } else {
//      println("Unable to connect to Alchemist")
//    }

//    requestWorkers(2)

//    stop
  }

  def initialize(_spark: SparkSession): this.type = {
    spark = _spark

    this
  }

  def createRandomRDD(nRows: Int, nCols: Int, ss: SparkSession): RDD[Array[Array[Double]]] = {
    val randomArray = Array.ofDim[Double](nRows,nCols)

    val rnd = new Random(123)

    for {
      i <- 0 until 20
      j <- 0 until 10
    } randomArray(i)(j) = rnd.nextGaussian()

    ss.sparkContext.parallelize(Seq(randomArray))
  }

  def loadLibrary(name: String, path: String, jar: String): LibraryHandle = {

    println(s"Loading library $name at $path")
    try {
      val lh = driver.loadLibrary(name, path)
      libraries = libraries + (lh.id -> lh)

      println(s"Loaded library $name with ID ${lh.id}")

      return lh
    }
    catch {
      case e: LibraryNotFoundException => { println(e.getMessage)
        return LibraryHandle(0, "", "") }
    }

//    var classLoader = new java.net.URLClassLoader(
//      Array(new File(jar).toURI.toURL),
//      this.getClass.getClassLoader)
//
//    var c = classLoader.loadClass(s"${name.toLowerCase}.${name}")
  }

  def connect(_address: String = "", _port: Int = 0): this.type = {

    var address = _address
    var port = _port
    if (address == "" || port == 0) {
      println(s"Reading Alchemist address and port from file")
      try {
        val fstream: FileInputStream = new FileInputStream("connection.info")
        // Get the object of DataInputStream
        val in: JDataInputStream = new JDataInputStream(fstream)
        val br: BufferedReader = new BufferedReader(new InputStreamReader(in))
        address = br.readLine()
        port = Integer.parseInt(br.readLine)

        in.close() //Close the input stream
      }
      catch {
        case e: Exception => println("Got this unknown exception: " + e.getMessage)
      }
    }

    println(s"Connecting to Alchemist at $address:$port")
    try {
      connected = driver.connect(address, port)
    }
    catch {
      case e: InvalidHandshakeException => println(e.getMessage)
    }

    this
  }

  def requestWorkers(numWorkers: Short): this.type = {

    if (connected) {
      if (numWorkers < 1)
        println(s"Cannot request $numWorkers Alchemist workers")
      else {
        println(s"Requesting $numWorkers Alchemist workers")

        workers = driver.requestWorkers(numWorkers)

        if (workers.isEmpty)
          println(s"Alchemist could not assign $numWorkers workers")
        else {
          println(s"Assigned $numWorkers workers:")
          workers.foreach(w => println(s"    ${w.toString}"))
        }
      }
    }

    this
  }

  def yieldWorkers(): this.type = {

    if (connected) driver.yieldWorkers()

    this
  }

  def runTask(lib: LibraryHandle, name: String, inArgs: Parameters): Parameters = {

    print(s"Alchemist started task '$name' ... ")
    driver.runTask(lib, name, inArgs)
  }

  def getArrayHandle(mat: DistributedMatrix, name: String = ""): ArrayHandle =
    driver.sendArrayInfo(name, mat.numRows, mat.numCols)

  def sendIndexedRowMatrix(mat: IndexedRowMatrix): ArrayHandle = {

    val ah: ArrayHandle = getArrayHandle(mat)

    val ahID: ArrayID = ah.id
    val ahNumPartitions: Long = ah.numPartitions.toLong
    val ahWorkerAssignments = ah.workerAssignments

    println(s"POL ${ah.numPartitions}")

    mat.rows.mapPartitionsWithIndex { (idx, part) =>
      val rows = part.toArray

      rows foreach { r => println(s"${idx} Rows ${r}") }

      var workerClients: Map[Short, WorkerClient] = Map.empty[Short, WorkerClient]

      var connectedWorkers: Int = 0
      workers foreach (w => {
        val wc: WorkerClient = new WorkerClient(w._2.ID, w._2.hostname, w._2.address, w._2.port)
        workerClients += (w._1 -> wc)
//        if (wc.connect) connectedWorkers += 1
      })

      workerClients foreach (w => if (w._2.connect) connectedWorkers += 1)

      if (connectedWorkers == workerClients.size) {
        println(s"Connected to ${connectedWorkers} workers")

        workerClients foreach (w => {
          println(s"tt ${w._2.clientID} ${w._2.ID}")
          w._2.startSendIndexedRows(ahID)
        })

        rows foreach (row => {
          val default = (0.toShort, 0l)
          val wID: Short = ahWorkerAssignments.find(_._2 == (row.index % ahNumPartitions)).getOrElse(default)._1
          println(s"vvvvvv to ${row.index} ${wID}")
          workerClients(wID).addIndexedRow(row)
        })

        workerClients foreach (w => {
          println(s"uu ${w._2.clientID} ${w._2.ID}")
          w._2.finishSendIndexedRows
        })

        println(s"Finished sending data")
      }

      println(s"Finished sending data 1")
      part
    }.count
    println(s"Finished sending data3")

    ah
  }

  def getIndexedRowMatrix(mh: ArrayHandle): IndexedRowMatrix = {
    // Generate random dataset
    val sc = spark.sparkContext
    val r = new scala.util.Random(1000L)

    val startTime = System.nanoTime()

    val indexedRows: RDD[IndexedRow] = sc.parallelize((0L to mh.numRows - 1)
      .map(x => new IndexedRow(x, new DenseVector(Array.fill(mh.numCols.toInt)(r.nextDouble())))))

    val data = new IndexedRowMatrix(indexedRows)

    println(s"Time to generate data: ${(System.nanoTime() - startTime) * 1.0E-9}")
    println(" ")

    data
  }

//    : IndexedRowMatrix = {

//    val layout: RDD[IndexedRow] = spark.sparkContext.parallelize(
//      (0l until mh.numRows).map(i => {
//        new IndexedRow(i, new DenseVector(new Array[Double](1)))
//      }), spark.sparkContext.defaultParallelism)
//
//    val indexedRows: RDD[IndexedRow] = layout.mapPartitionsWithIndex({ (idx, part) =>
//      val rows = part.toArray
//
//      var connected: Int = 0
//      var workers: Map[Byte, WorkerClient] = Map.empty[Byte, WorkerClient]
//
//      workers.foreach(w => workers += (w._1 -> new WorkerClient(w._1, w._2.hostname, w._2.address, w._2.port)))
//
//      workers.foreach(w => {
//        if (w._2.connect()) connected += 1
//      })
//
//      var currentRowIter: Iterator[IndexedRow] = rows.map(row => new IndexedRow(row.index, row.vector)).iterator
//
//      if (connected == workers.size) {
//        workers.foreach(w => w._2.startRequestArrayBlocks(mh.id.value))
//
//        rows.foreach(row => {
//          val index: Long = row.index
//          workers(mh.workerLayout(index.toInt)).addRequestedArrayBlock(Array(index,index,0,mh.numCols-1))
//        })
//
//        workers.foreach(w => w._2.finishRequestArrayBlocks)
//
//        currentRowIter = rows.map(row => {
//          val index: Long = row.index
//          new IndexedRow(index, workers(mh.workerLayout(index.toInt)).getRequestedArrayBlock(Array(index,index,0,mh.numCols-1)))
//        }).iterator
//
//        workers.foreach(w => w._2.disconnectFromAlchemist)
//      }
//
//      currentRowIter
//    }, preservesPartitioning = true)

//    new IndexedRowMatrix(indexedRows, mh.numRows, mh.numCols.toInt)
//  }

  def sendRowMatrix(mat: RowMatrix): ArrayHandle = getArrayHandle(mat)

  def sendTestString: this.type = {
    println("Sending test string to Alchemist")
    driver.sendTestString("This is a test string from ACISpark")

    this
  }

  def printIndexedRowMatrix(mat: IndexedRowMatrix): this.type = {

    mat.rows.mapPartitionsWithIndex((idx, iterator) => iterator.map( v => (idx, v))).foreach(v => println(v))

    this
  }

  def requestTestString: this.type = {
    println("Requesting test string from Alchemist")
    println(s"Received test string '${driver.requestTestString}'")

    this
  }

  def yieldWorkers(yieldedWorkers: List[Byte] = List.empty[Byte]): this.type = {
    val deallocatedWorkers: List[Byte] = driver.yieldWorkers(yieldedWorkers)

    if (deallocatedWorkers.length == 0) {
      println("No workers were deallocated\n")
    }
    else {
      print("Deallocated workers ")
      deallocatedWorkers.zipWithIndex.foreach {
        case (w, i) => {
          if (i < deallocatedWorkers.length) print(s"$w, ")
          else print(s"and $w")
        }
      }
    }

    this
  }

  def listAlchemistWorkers: this.type = printWorkers(driver.listAllWorkers)

  def listAllWorkers: this.type = printWorkers(driver.listAllWorkers)

  def listInactiveWorkers: this.type = printWorkers(driver.listInactiveWorkers, "inactive ")

  def listActiveWorkers: this.type = printWorkers(driver.listActiveWorkers, "active ")

  def listAssignedWorkers: this.type = printWorkers(driver.listAssignedWorkers, "assigned ")

  def printWorkers(workerList: Array[WorkerInfo], workerType: String = ""): this.type = {

    workerList.length match {
      case 0 => println(s"No ${workerType}workers")
      case 1 => println(s"Listing 1 ${workerType}worker")
      case _ => println(s"Listing ${workerList.length} ${workerType}workers")
    }

    workerList foreach { w => println(s"    ${w.toString(true)}") }

    this
  }

  def stop: this.type = {

    yieldWorkers
    println("Ending Alchemist session")

    this
  }

  def close: this.type = {
    driver.close
//    workers.foreach(p => p._2.close)

    this
  }

  //  def setSparkContext(_sc: SparkContext) {
  //    sc = _sc
  //  }
  //
  //  def run(libraryName: String, funName: String, inputParams: Parameters): Parameters = {
  //    client.runCommand(libraryName, funName, inputParams)
  //  }
//
//  // Caches result by default, because may not want to recreate (e.g. if delete referenced matrix on Alchemist side to save memory)
//  def getIndexedRowMatrix(handle: ArrayHandle): IndexedRowMatrix = {
//    val (numRows, numCols) = getDimensions(handle)
//    // TODO:
//    // should map the rows back to the executors using locality information if possible
//    // otherwise shuffle the rows on the MPI side before sending them back to SPARK
//    val numPartitions = max(sc.defaultParallelism, client.workerCount)
//    val sacrificialRDD = sc.parallelize(0 until numRows.toInt, numPartitions)
//    val layout: Array[WorkerId] = (0 until sacrificialRDD.partitions.size).map(x => new WorkerId(x % client.workerCount)).toArray
//    val fullLayout: Array[WorkerId] = (layout zip sacrificialRDD.mapPartitions(iter => Iterator.single(iter.size), true)
//                                          .collect())
//                                          .flatMap{ case (workerid, partitionSize) => Array.fill(partitionSize)(workerid) }
//
//    client.getIndexedRowMatrix(handle, fullLayout)
//    val rows = sacrificialRDD.mapPartitionsWithIndex( (idx, rowindices) => {
//      val worker = context.connectWorker(layout(idx))
//      val result = rowindices.toList.map { rowIndex =>
//        new IndexedRow(rowIndex, worker.getIndexedRowMatrixRow(handle, rowIndex, numCols))
//      }.iterator
//      worker.close()
//      result
//    }, preservesPartitioning = true)
//    val result = new IndexedRowMatrix(rows, numRows, numCols)
//    result.rows.cache()
//    result.rows.count
//    result
//  }
//
//  def readHDF5(fname: String, varname: String): ArrayHandle = client.readHDF5(fname, varname)
//
//  def getDimensions(handle: ArrayHandle): Tuple2[Long, Int] = client.getArrayDimensions(handle)
//
//  def transpose(mat: IndexedRowMatrix): IndexedRowMatrix = {
//    getIndexedRowMatrix(client.getTranspose(getArrayHandle(mat)))
//  }
//
//  def matrixMultiply(matA: IndexedRowMatrix, matB: IndexedRowMatrix): (IndexedRowMatrix, Array[Double]) = {
//
//    var t1 = System.nanoTime()
//    val handleA = getArrayHandle(matA)
//    val handleB = getArrayHandle(matB)
//    val t2 = System.nanoTime() - t1
//
//    t1 = System.nanoTime()
//    val handleC = client.matrixMultiply(handleA, handleB)
//    val t3 = System.nanoTime() - t1
//
//    t1 = System.nanoTime()
//    val matC = getIndexedRowMatrix(handleC)
//    val t4 = System.nanoTime() - t1
//
//    (matC, Array(t2, t3, t4))
//  }
//
//  def stop() = driver.stop()
}
