package alchemist

@SerialVersionUID(13L)
class MatrixBlock(
  val data: Array[Double] = Array.empty[Double],
  val rows: Array[Long] = Array(0L, 0L, 1L),
  val cols: Array[Long] = Array(0L, 0L, 1L)
) {

  def toString(space: String = ""): String = {
    var dataStr = ""

    var k: Long = 0

    dataStr = dataStr.concat(s"Rows: ${rows(0)} ${rows(1)} ${rows(2)}\n$space")
    dataStr = dataStr.concat(s"Cols: ${cols(0)} ${cols(1)} ${cols(2)}\n$space")

    for (_ <- rows(0) to rows(1) by rows(2)) {
      for (_ <- cols(0) to cols(1) by cols(2)) {
        dataStr = dataStr.concat(s"${data(k.toInt)} ")
        k += 1
      }
      dataStr = dataStr.concat(s"\n$space")
    }

    dataStr
  }
}
