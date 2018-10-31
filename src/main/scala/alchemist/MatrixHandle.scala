package alchemist

import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import scala.math.max
import org.apache.spark.sql.SparkSession

class MatrixHandle(val id: Short, val numRows: Long, val numCols: Long) extends Serializable {

  var rowLayout: Array[Short] = Array.empty[Short]
  var workerLayout: Map[Short, Array[Long]] = Map.empty[Short, Array[Long]]

  def getDimensions: Tuple2[Long, Long] = {
    (numRows, numCols)
  }

  def setRowLayout(_rowLayout: Array[Short]): this.type = {
    rowLayout = _rowLayout

    var i: Int = 0
    for (i <- rowLayout.indices) println(s"RR ${i} ${rowLayout(i)}")

    this
  }

  def getIndexedRowMatrix(ss: SparkSession): IndexedRowMatrix = {
    AlchemistSession.getIndexedRowMatrix(this, ss.sparkContext)
  }
}