package alchemist

import scala.io.Source
import alchemist.io._
import org.apache.spark.mllib.linalg.distributed.IndexedRow

import scala.collection.mutable.ArrayBuffer
import scala.collection.Map
import scala.collection.immutable.Seq
import scala.util.Random

// spark-core
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
// spark-sql
import org.apache.spark.sql.SparkSession
// spark-mllib
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}
import scala.math.max

//class AlchemistContext(client: DriverSession) extends Serializable {
//
////  val workerIds: Array[WorkerId] = client.workerIds
////  val workerInfo: Array[WorkerInfo] = client.workerInfo
//
//  def connectWorker(worker: WorkerId): WorkerClient = workerInfo(worker.id).connect
//}

object AlchemistSession {
  println("Launching Alchemist")


  var connected: Boolean = false

  var driverClient: DriverClient = _
  var workerClients: Map[Short, WorkerClient] = Map.empty[Short, WorkerClient]

  def main(args: Array[String]) {

    initialize
    connected = connect("0.0.0.0", 24960)
    if (connected) {
      println("Connected to Alchemist")
    } else {
      println("Unable to connect to Alchemist")
    }

    requestWorkers(2)

    stop
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

//  var client: DriverClient = _
//
//  var sc: SparkContext = _
//
//  var context: AlchemistContext = _
//
//  val libraries = collection.mutable.Map[String, String]()
//
//  def registerLibrary(libraryInfo: (String, String)) {
//    libraries.update(libraryInfo._1, libraryInfo._2)
//    client.loadLibrary(libraryInfo._1, libraryInfo._2)
//  }
//
//  def listLibraries(): Unit = libraries foreach (x => println (x._1 + "-->" + x._2))
//
  def initialize(): this.type = {

    driverClient = new DriverClient()

//    client = driver.client

    // Instances of `Alchemist` are not serializable, but `.context` has everything needed for RDD operations
    // and is serializable
//    context = new AlchemistContext(client)

    this
  }

  def connect(address: String, port: Int): Boolean = {

    driverClient.connect(address, port)
  }

  def requestWorkers(numWorkers: Short): this.type = {

    workerClients = driverClient.requestWorkers(numWorkers)

    if (workerClients.isEmpty)
      println(s"Alchemist could not assign $numWorkers workers")

    this
  }

  def yieldWorkers(): this.type = {

    driverClient.yieldWorkers

    this
  }

  def getMatrixHandle(mat: IndexedRowMatrix): MatrixHandle = driverClient.sendMatrixInfo(mat.numRows, mat.numCols)

  def sendIndexedRowMatrix(mat: IndexedRowMatrix): MatrixHandle = {

    val mh = getMatrixHandle(mat)

    mat.rows.mapPartitionsWithIndex { (idx, part) =>
      val rows = part.toArray

      var connected: Int = 0
      var workers: Map[Short, WorkerClient] = Map.empty[Short, WorkerClient]

      workerClients.foreach(w => workers += (w._1 -> new WorkerClient(w._1, w._2.hostname, w._2.address, w._2.port)))

      workers.foreach(w => {
        if (w._2.connect()) connected += 1
      })

      if (connected == workers.size) {
        workers.foreach(w => w._2.startSendMatrixBlocks(mh.id))

        rows.foreach(row => {
          lazy val elements = row.vector.toArray
          val index: Long = row.index
          workers(mh.rowLayout(index.toInt)).addSendMatrixBlock(Array(index,index,0,elements.length-1), elements)
        })

        workers.foreach(w => w._2.finishSendMatrixBlocks.disconnectFromAlchemist)
      }

      part
    }.count

    mh
  }

  def getIndexedRowMatrix(mh: MatrixHandle, sc: SparkContext): IndexedRowMatrix = {

    val layout: RDD[IndexedRow] = sc.parallelize(
      (0l until mh.numRows).map(i => {
        new IndexedRow(i, new DenseVector(new Array[Double](1)))
      }), sc.defaultParallelism)

    val indexedRows: RDD[IndexedRow] = layout.mapPartitionsWithIndex({ (idx, part) =>
      val rows = part.toArray

      var connected: Int = 0
      var workers: Map[Short, WorkerClient] = Map.empty[Short, WorkerClient]

      workerClients.foreach(w => workers += (w._1 -> new WorkerClient(w._1, w._2.hostname, w._2.address, w._2.port)))

      workers.foreach(w => {
        if (w._2.connect()) connected += 1
      })

      var currentRowIter: Iterator[IndexedRow] = rows.map(row => new IndexedRow(row.index, row.vector)).iterator

      if (connected == workers.size) {
        workers.foreach(w => w._2.startRequestMatrixBlocks(mh.id))

        rows.foreach(row => {
          val index: Long = row.index
          workers(mh.rowLayout(index.toInt)).addRequestedMatrixBlock(Array(index,index,0,mh.numCols-1))
        })

        workers.foreach(w => w._2.finishRequestMatrixBlocks)

        currentRowIter = rows.map(row => {
          val index: Long = row.index
          new IndexedRow(index, workers(mh.rowLayout(index.toInt)).getRequestedMatrixBlock(Array(index,index,0,mh.numCols-1)))
        }).iterator

        workers.foreach(w => w._2.disconnectFromAlchemist)
      }

      currentRowIter
    }, preservesPartitioning = true)

    new IndexedRowMatrix(indexedRows, mh.numRows, mh.numCols.toInt)
  }

  def sendRowMatrix(mat: RowMatrix): MatrixHandle = getMatrixHandle(mat)

  def getMatrixHandle(mat: RowMatrix): MatrixHandle = driverClient.sendMatrixInfo(mat.numRows, mat.numCols)

  def stop(): this.type = {

    yieldWorkers
    println("Ending Alchemist session")

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
//  def getIndexedRowMatrix(handle: MatrixHandle): IndexedRowMatrix = {
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
//  def readHDF5(fname: String, varname: String): MatrixHandle = client.readHDF5(fname, varname)
//
//  def getDimensions(handle: MatrixHandle): Tuple2[Long, Int] = client.getMatrixDimensions(handle)
//
//  def transpose(mat: IndexedRowMatrix): IndexedRowMatrix = {
//    getIndexedRowMatrix(client.getTranspose(getMatrixHandle(mat)))
//  }
//
//  def matrixMultiply(matA: IndexedRowMatrix, matB: IndexedRowMatrix): (IndexedRowMatrix, Array[Double]) = {
//
//    var t1 = System.nanoTime()
//    val handleA = getMatrixHandle(matA)
//    val handleB = getMatrixHandle(matB)
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
