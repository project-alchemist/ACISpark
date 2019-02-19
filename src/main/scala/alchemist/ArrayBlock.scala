package alchemist

class ArrayBlockDouble(val dims: Array[Array[Long]] = Array.empty[Array[Long]],
                               val data: Array[Double] = Array.empty[Double]) {

  val nnz: Long = data.size.toLong

  def toString(space: String = ""): String = {
    var dataStr = ""

    var size: Long = 1l
    dims.foreach(dim => size *= (dim(1) - dim(0) + 1l))

    val tempData = Array.fill[Double](size.toInt)(0.0).grouped((dims(1)(1)-dims(1)(0)+1).toInt).toArray

    for (i <- dims(0)(0) to dims(0)(1) by dims(0)(2)) {
      for (j <- dims(1)(0) to dims(1)(1) by dims(1)(2))
        tempData(i.toInt)(j.toInt) = data((i*(dims(0)(1)- dims(0)(0)) + j).toInt)
    }

    for (i <- dims(0)(0) to dims(0)(1)) {
      for (j <- dims(1)(0) to dims(1)(1))
        dataStr = dataStr.concat(s"${tempData(i.toInt)(j.toInt)} ")
      dataStr = dataStr.concat(s"\n$space")
    }

    dataStr
  }

}


class ArrayBlockFloat(val dims: Array[Array[Long]] = Array.empty[Array[Long]],
                       val data: Array[Float] = Array.empty[Float]) {

  val nnz: Long = data.size.toLong

  def toString(space: String = ""): String = {
    var dataStr = ""

    var size: Long = 1l
    dims.foreach(dim => size *= (dim(1) - dim(0) + 1l))

    val tempData = Array.fill[Float](size.toInt)(0.0.asInstanceOf[Float]).grouped((dims(1)(1)-dims(1)(0)+1).toInt).toArray

    for (i <- dims(0)(0) to dims(0)(1) by dims(0)(2)) {
      for (j <- dims(1)(0) to dims(1)(1) by dims(1)(2))
        tempData(i.toInt)(j.toInt) = data((i*(dims(0)(1)- dims(0)(0)) + j).toInt)
    }

    for (i <- dims(0)(0) to dims(0)(1)) {
      for (j <- dims(1)(0) to dims(1)(1))
        dataStr = dataStr.concat(s"${tempData(i.toInt)(j.toInt)} ")
      dataStr = dataStr.concat(s"\n$space")
    }

    dataStr
  }

}
