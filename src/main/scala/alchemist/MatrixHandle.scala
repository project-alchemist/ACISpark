package alchemist

import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import scala.math.max
import org.apache.spark.sql.SparkSession

class MatrixHandle(val id: Short, val numRows: Long, val numCols: Long, val layout: Array[Short]) {

  def getDimensions: Tuple2[Long, Long] = {
    (numRows, numCols)
  }

//
//  def transpose() : AlMatrix = {
//    new AlMatrix(al, al.client.getTranspose(handle))
//  }
//
  // Caches result by default, because may not want to recreate (e.g. if delete referenced matrix on Alchemist side to save memory)
  def getIndexedRowMatrix(ss: SparkSession): this.type = {
//    val (numRows, numCols) = getDimensions()
//    // TODO:
//    // should map the rows back to the executors using locality information if possible
//    // otherwise shuffle the rows on the MPI side before sending them back to SPARK
//    val numPartitions = max(al.sc.defaultParallelism, al.client.workerCount)
//    val sacrificialRDD = al.sc.parallelize(0 until numRows.toInt, numPartitions)
//    val layout : Array[WorkerId] = (0 until sacrificialRDD.partitions.size).map(x => new WorkerId(x % al.client.workerCount)).toArray
//    val full_layout : Array[WorkerId] = (layout zip sacrificialRDD.mapPartitions(iter => Iterator.single(iter.size), true).collect()).
//                                          flatMap{ case (workerid, partitionSize) => Array.fill(partitionSize)(workerid) }
//    // capture references needed by the closure without capturing `this.al`
//    val ctx = al.context
//    val handle = this.handle
//
//    al.client.getIndexedRowMatrixStart(handle, full_layout)
//    val rows = sacrificialRDD.mapPartitionsWithIndex( (idx, rowindices) => {
//      val worker = ctx.connectWorker(layout(idx))
//      val result  = rowindices.toList.map { rowIndex =>
//        new IndexedRow(rowIndex, worker.getIndexedRowMatrix_getRow(handle, rowIndex, numCols))
//      }.iterator
//      worker.close()
//      result
//    }, preservesPartitioning=true)
//    val result = new IndexedRowMatrix(rows, numRows, numCols)
//    result.rows.cache()
//    result.rows.count
//    al.client.getIndexedRowMatrixFinish(handle)
//    result
    this
  }
}