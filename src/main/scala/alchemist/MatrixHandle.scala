package alchemist

import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import scala.math.max
import org.apache.spark.sql.SparkSession

class MatrixHandle(val id: Short = 0, val name: String = "", val numRows: Long = 0, val numCols: Long = 0,
                   val sparse: Boolean = false, val numPartitions: Byte = 0,
                   val rowLayout: Array[Byte] = Array.empty[Byte]) {

  def getID: Short = id

  def getName: String = name

  def getNumRows: Long = numRows

  def getNumCols: Long = numCols

  def getDimensions = (numRows, numCols)

  def getSparse: Boolean = sparse

  def getNumPartitions: Byte = numPartitions

  def getRowLayout: Array[Byte] = rowLayout

  def getIndexedRowMatrix(ss: SparkSession): IndexedRowMatrix = {
    AlchemistSession.getIndexedRowMatrix(this, ss.sparkContext)
  }

  def meta(displayLayout: Boolean = false): this.type = {

    println(s"Name:                  $name")
    println(s"ID:                    $id")
    println(" ")
    println(s"Number of rows:        $numRows")
    println(s"Number of columns:     $numCols")
    println(" ")
    println(s"Sparse:                $sparse")
    println(s"Number of partitions:  $numPartitions")
    if (display_layout) {
      print(" ")
      print(s"Layout:")
      for (i <- rowLayout.indices) println(s"    ${i} ${rowLayout(i)}")
    }
  }
}