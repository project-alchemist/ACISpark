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
    val randomArray = Array.ofDim[Double](nRows, nCols)

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
      case e: LibraryNotFoundException => {
        println(e.getMessage)
        return LibraryHandle(0, "", "")
      }
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
    val outArgs: Parameters = driver.runTask(lib, name, inArgs)
    println("done")

    outArgs
  }

  def getArrayHandle(mat: DistributedMatrix, name: String = ""): ArrayHandle =
    driver.sendArrayInfo(name, mat.numRows, mat.numCols)

  def sendIndexedRowMatrix(mat: IndexedRowMatrix): ArrayHandle = {

    val ah: ArrayHandle = getArrayHandle(mat)

    val ahID: ArrayID = ah.id
    val ahNumPartitions: Long = ah.numPartitions.toLong
    val ahWorkerAssignments = ah.workerAssignments

    mat.rows.mapPartitionsWithIndex { (idx, part) =>
      val rows = part.toArray

      var workerClients: Map[Short, WorkerClient] = Map.empty[Short, WorkerClient]

      var connectedWorkers: Int = 0
      workers foreach (w => {
        val wc: WorkerClient = new WorkerClient(w._2.ID, w._2.hostname, w._2.address, w._2.port)
        workerClients += (w._1 -> wc)
        if (wc.connect(idx)) connectedWorkers += 1
      })

      if (connectedWorkers == workerClients.size) {
        println(s"Spark executor ${idx}: Connected to ${connectedWorkers} Alchemist workers")

        workerClients foreach (w => {
          w._2.startSendIndexedRows(ahID)
        })

        rows foreach (row => {
          val default = (0.toShort, 0l)
          val wID: Short = ahWorkerAssignments.find(_._2 == (row.index % ahNumPartitions)).getOrElse(default)._1
          workerClients(wID).addIndexedRow(row)
        })

        workerClients foreach (w => {
          val numRows: Long = w._2.finishSendIndexedRows

          println(s"Spark executor ${idx}: Alchemist received $numRows rows for Array ${ahID.value}")
        })
      }

      println(s"Spark executor ${idx}: Finished sending data")

      part
    }.count

    println(s"Finished sending data")

    ah
  }

  def getIndexedRowMatrix(ah: ArrayHandle): IndexedRowMatrix = {

    val rowIndices: RDD[Long] = spark.sparkContext.parallelize(0l until ah.numRows)

    val ahID: ArrayID = ah.id
    val ahNumPartitions: Long = ah.numPartitions.toLong
    val ahWorkerAssignments = ah.workerAssignments

    new IndexedRowMatrix(
      spark.sparkContext.union(
        rowIndices.mapPartitionsWithIndex { (idx, part) => {
          val rows = part.toArray

          var workerClients: Map[Short, WorkerClient] = Map.empty[Short, WorkerClient]

          var connectedWorkers: Int = 0
          workers foreach (w => {
            val wc: WorkerClient = new WorkerClient(w._2.ID, w._2.hostname, w._2.address, w._2.port)
            workerClients += (w._1 -> wc)
            if (wc.connect(idx)) connectedWorkers += 1
          })

          var requestedIndexedRows: Array[IndexedRow] = Array.empty[IndexedRow]

          if (connectedWorkers == workerClients.size) {
            println(s"Spark executor ${idx}: Connected to ${connectedWorkers} Alchemist workers")

            workerClients foreach (w => {
              w._2.startRequestIndexedRows(ahID)
            })

            rows foreach (row => {
              val default = (0.toShort, 0l)
              val wID: Short = ahWorkerAssignments.find(_._2 == (row % ahNumPartitions)).getOrElse(default)._1
              workerClients(wID).requestIndexedRow(row)
            })

            workerClients foreach (w => {
              val returnedRows: Array[IndexedRow] = w._2.finishRequestIndexedRows

              var row_text: String = "row"
              if (returnedRows.length > 1)
                row_text += "s"
              println(s"Spark executor ${idx}: Alchemist worker ${w._2.ID} returned ${returnedRows.length} ${row_text} for Array ${ahID.value}")
              requestedIndexedRows = requestedIndexedRows ++ returnedRows
            })

            println(s"Spark executor ${idx}: Finished receiving data")
          }

          requestedIndexedRows.iterator
        }
        }))
  }

  def sendRowMatrix(mat: RowMatrix): ArrayHandle = getArrayHandle(mat)

  def sendTestString: this.type = {
    println("Sending test string to Alchemist")
    driver.sendTestString("This is a test string from ACISpark")

    this
  }

  def printIndexedRowMatrix(mat: IndexedRowMatrix): this.type = {

    mat.rows.mapPartitionsWithIndex((idx, iterator) => iterator.map(v => (idx, v))).foreach(v => println(v))

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

    println("Ending Alchemist session")
    driver.close

    this
  }
}
