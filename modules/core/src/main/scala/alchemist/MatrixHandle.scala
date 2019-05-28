package alchemist

import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import scala.math.max
import org.apache.spark.sql.SparkSession
@SerialVersionUID(16L)
class ProcessGrid(
    val numRows: Short = 0,
    val numCols: Short = 0,
    val array: Map[Short, Array[Short]] = Map.empty[Short, Array[Short]]
) extends Serializable
@SerialVersionUID(15L)
class MatrixHandle(
    val id: MatrixID = MatrixID(0),
    val name: String = "",
    val numRows: Long = 0,
    val numCols: Long = 0,
    val sparse: Byte = 0,
    val layout: Byte = Layout.MC_MR.value,
    val grid: ProcessGrid = new ProcessGrid
) extends Serializable {

  def getID: MatrixID = id

  def getName: String = name

  def getNumRows: Long = numRows

  def getNumCols: Long = numCols

  def getDimensions = (numRows, numCols)

  def getSparse: Byte = sparse

  def getNumPartitions: Short = (grid.numRows * grid.numCols).toShort

  def getGrid: ProcessGrid = grid

  def getRowAssignments(ID: Short): Array[Long] = {

    var rows: Array[Long] = Array.empty[Long]

    if (layout == Layout.MC_MR.value)
      rows = Array(grid.array(ID)(0).toLong, numRows - 1, grid.numRows.toLong)
    else if (layout == Layout.MR_MC.value)
      rows = Array(grid.array(ID)(1).toLong, numRows - 1, grid.numCols.toLong)
    else if (layout == Layout.VC_STAR.value)
      rows = Array(grid.array(ID)(1).toLong, numRows - 1, (grid.numCols * grid.numRows).toLong)
    else if (layout == Layout.VR_STAR.value)
      rows = Array(grid.array(ID)(1).toLong, numRows - 1, 1L)

    rows
  }

  def getColAssignments(ID: Short): Array[Long] = {

    var cols: Array[Long] = Array.empty[Long]

    if (layout == Layout.MC_MR.value)
      cols = Array(grid.array(ID)(1).toLong, numCols - 1, grid.numCols.toLong)
    else if (layout == Layout.MR_MC.value)
      cols = Array(grid.array(ID)(1).toLong, numCols - 1, grid.numRows.toLong)
    else if (layout == Layout.VC_STAR.value)
      cols = Array(grid.array(ID)(1).toLong, numCols - 1, 1L)
    else if (layout == Layout.VR_STAR.value)
      cols = Array(grid.array(ID)(1).toLong, numCols - 1, (grid.numCols * grid.numRows).toLong)

    cols
  }

  def getIndexedRowMatrix: IndexedRowMatrix = AlchemistSession.getIndexedRowMatrix(this)

  def toString(space: String = ""): String = {

    var dataStr = ""

    dataStr = dataStr.concat(s"\n$space Name:                  $name\n")
    dataStr = dataStr.concat(s"$space ID:                    ${id.value}\n\n")
    dataStr = dataStr.concat(s"$space Number of rows:        $numRows\n")
    dataStr = dataStr.concat(s"$space Number of columns:     $numCols\n\n")
    dataStr = dataStr.concat(s"$space Sparse:                $sparse\n")
    dataStr = dataStr.concat(s"$space Layout:                ${Layout.withValue(layout).label}\n\n")
    dataStr = dataStr.concat(s"$space Grid (${grid.numRows} x ${grid.numCols}):\n")
    grid.array foreach {
      case (k, v) => dataStr = dataStr.concat(s"$space                        ${k}: ${v(0)}, ${v(1)}\n")
    }

    dataStr
  }
}
