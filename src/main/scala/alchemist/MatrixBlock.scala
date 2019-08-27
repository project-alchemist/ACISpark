package alchemist

@SerialVersionUID(13L)
class MatrixBlock(val data: Array[Double],
                  val rows: (Long, Long, Long) = (0l, 0l, 1l),
                  val cols: (Long, Long, Long) = (0l, 0l, 1l)) {

  def this(data: Array[Double], r: Long, c: Long) {
    this(data, (r, r, 1l), (c, c, 1l))
  }

  def this(data: Array[Double], r: Long, cols: (Long, Long, Long)) {
    this(data, (r, r, 1l), cols)
  }

  def this(data: Array[Double], rows: (Long, Long, Long), c: Long) {
    this(data, rows, (c, c, 1l))
  }

  def this(data: Array[Double], _rowStart: Long, _rowEnd: Long, _rowStep: Long,
           _colStart: Long, _colEnd: Long, _colStep: Long) {
    this(data, (_rowStart, _rowEnd, _rowStep), (_colStart, _colEnd, _colStep))
  }

  val rowStart: Long = rows._1
  val rowEnd: Long = rows._2
  val rowStep: Long = rows._3
  val colStart: Long = cols._1
  val colEnd: Long = cols._2
  val colStep: Long = cols._3

  def toString(space: String = "", printData: Boolean = false): String = {
    var dataStr = ""

    var k: Long = 0

    dataStr = dataStr.concat(s"Rows: ${rowStart} ${rowEnd} ${rowStep}\n$space")
    dataStr = dataStr.concat(s"Cols: ${colStart} ${colEnd} ${colStep}\n$space")

    if (data.length == 0) {
      dataStr = dataStr.concat(s"MatrixBlock is empty\n$space")
    }
    else {
      if (!printData)
        dataStr = dataStr.concat(s"Not displaying data\n$space")
      else {
        for (_ <- rows._1 to rows._2 by rows._3) {
          for (_ <- cols._1 to cols._2 by cols._3) {
            dataStr = dataStr.concat(s"${data(k.toInt)} ")
            k += 1
          }
          dataStr = dataStr.concat(s"\n$space")
        }
      }
    }

    dataStr
  }
}
