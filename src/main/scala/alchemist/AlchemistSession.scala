package alchemist

import scala.io.Source
import alchemist.io._

// spark-core
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
// spark-sql
import org.apache.spark.sql.SparkSession
// spark-mllib
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}
import scala.math.max

class AlchemistContext(client: DriverSession) extends Serializable {
  
  val workerIds: Array[WorkerId] = client.workerIds
  val workerInfo: Array[WorkerInfo] = client.workerInfo

  def connectWorker(worker: WorkerId): WorkerClient = workerInfo(worker.id).connect
}

object AlchemistSession {
  println("Launching Alchemist")
  
  var driver: Driver = new Driver()

  var connected: Boolean = false
  
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

//    client = driver.client

    // Instances of `Alchemist` are not serializable, but `.context` has everything needed for RDD operations
    // and is serializable
//    context = new AlchemistContext(client)

    this
  }

  def connect(address: String, port: Int): Boolean = {
    driver.connect(address, port)
  }

  def requestWorkers(numWorkers: Short): this.type = {
    driver.requestWorkers(numWorkers)

    this
  }

  def yieldWorkers(): this.type = {

    driver.yieldWorkers

    this
  }

  def getMatrixHandle(mat: IndexedRowMatrix): MatrixHandle = {

    val mh: MatrixHandle = driver.sendMatrixInfo(mat.numRows, mat.numCols)

//    val workerAssignments: collection.mutable.Map[Short, Array[Long]] = _



//    mat.rows.mapPartitionsWithIndex { (id, part) =>
//      val localRows = part.toArray
//      val relevantWorkers = localRows.map(row => mh.layout(row.index.toInt)).distinct.map(id => new WorkerId(id))
//      println("Sending data to following workers: ")
//      println(relevantWorkers.map(node => node.id.toString).mkString(" "))
//      val maxWorkerId = relevantWorkers.map(node => node.id).max
//      var nodeClients = Array.fill(maxWorkerId+1)(None: Option[WorkerClient])
//      System.err.println(s"Connecting to ${relevantWorkers.length} workers")
//      relevantWorkers.foreach(node => nodeClients(node.id) = Some(ctx.connectWorker(node)))
//      System.err.println(s"Successfully connected to all workers; have ${rows.length} rows to send")
//    }

    mh
  }

  def getMatrixHandle(mat: RowMatrix): MatrixHandle = {

    driver.sendMatrixInfo(mat.numRows, mat.numCols)
  }

  def stop(): this.type = {

    yieldWorkers
    println("Ending Alchemist session")
  }

//  def getMatrixHandle(mat: IndexedRowMatrix): MatrixHandle = {
//    val workerIds = context.workerIds
//    // rowWorkerAssignments is an array of WorkerIds whose ith entry is the world rank of the alchemist worker
//    // that will take the ith row (ranging from 0 to numworkers-1). Note 0 is an executor, not the driver
//    try {
//      val (handle, rowWorkerAssignments) = client.sendNewMatrix(mat.numRows, mat.numCols)
//
//      mat.rows.mapPartitionsWithIndex { (idx, part) =>
//        val rows = part.toArray
//        val relevantWorkers = rows.map(row => rowWorkerAssignments(row.index.toInt).id).distinct.map(id => new WorkerId(id))
//        val maxWorkerId = relevantWorkers.map(node => node.id).max
//        var nodeClients = Array.fill(maxWorkerId+1)(None: Option[WorkerClient])
//        System.err.println(s"Connecting to ${relevantWorkers.length} workers")
//        relevantWorkers.foreach(node => nodeClients(node.id) = Some(context.connectWorker(node)))
//        System.err.println(s"Successfully connected to all workers")
//
//        // TODO: randomize the order the rows are sent in to avoid queuing issues?
//        var count = 0
//        rows.foreach{ row =>
//          count += 1
//  //        System.err.println(s"Sending row ${row.index.toInt}, ${count} of ${rows.length}")
//          nodeClients(rowWorkerAssignments(row.index.toInt).id).get.
//            newMatrixAddRow(handle, row.index, row.vector.toArray)
//        }
//        System.err.println("Finished sending rows")
//        nodeClients.foreach(client =>
//            if (client.isDefined) {
//              client.get.newMatrixPartitionComplete(handle)
//              client.get.close()
//            })
//        Iterator.single(true)
//      }.count
//
//      client.sendNewMatrixDone()
//
//      handle
//    }
//    catch {
//      case protocol: ProtocolException => System.err.println("Protocol Exception in 'Alchemist.getMatrixHandle'")
//      stop()
//      new MatrixHandle(0)
//    }
//  }

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
