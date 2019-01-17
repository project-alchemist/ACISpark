package alchemist





class Parameter:

name = []
datatype = []
value = []

datatypes = {"BYTE": 33,
"SHORT": 34,
"INT": 35,
"LONG": 36,
"FLOAT": 15,
"DOUBLE": 16,
"CHAR": 1,
"STRING": 46,
"MATRIX_INFO": 52}


private[alchemist] abstract class Parameter(override val _name: String, override val _ctype: Datatype) {

  def getName(): String = _name

  def getDatatype(): Datatype = _ctype
}

class ByteParameter(val _name: String, val _ctype: Datatype, val _value: Byte) {

  def getValue(): Byte = _value
}

class ShortParameter(val _name: String, val _ctype: Datatype, val _value: Short) {

  def getValue(): Short = _value
}

class IntParameter(val _name: String, val _ctype: Datatype, val _value: Int) {

  def getValue(): Int = _value
}

class LongParameter(val _name: String, val _ctype: Datatype, val _value: Long) {

  def getValue(): Long = _value
}

class FloatParameter(val _name: String, val _ctype: Datatype, val _value: Float) {

  def getValue(): Float = _value
}

class DoubleParameter(val _name: String, val _ctype: Datatype, val _value: Double) {

  def getValue(): Double = _value
}

class CharParameter(val _name: String, val _ctype: Datatype, val _value: Char) {

  def getValue(): Char = _value
}

class StringParameter(val _name: String, val _ctype: Datatype, val _value: String) {

  def getValue(): String = _value
}

class MatrixInfoParameter(val _name: String, val _ctype: Datatype, val _value: MatrixHandle) {

  def getValue(): MatrixHandle = _value
}

class Parameters() {

//  val p = scala.collection.mutable.ArrayBuffer.empty[Parameter]
//
//  def addParameter(name: String, ctype: String, value: String): Unit = {
//    p += new Parameter(name, ctype, value)
//  }
//
//  override def toString(): String = {
//    val buf = new StringBuilder
//    p.foreach(arg => buf ++= arg.toString + " ")
//    buf.toString
//  }
//
//  def print(): Unit = {
//    p.foreach(println)
//  }
//
//  def getBoolean(name: String): Boolean = {
//    val b = p.find(_.getName == name).map(_.getValue).getOrElse("f")
//    if (b.toString() == "t")
//      true
//    else
//      false
//  }
//
//  def getInt(name: String): Int = {
//    p.find(_.getName == name).map(_.getValue).map(_.toInt).getOrElse(0)
//  }
//
//  def getLong(name: String): Long = {
//    p.find(_.getName == name).map(_.getValue).map(_.toLong).getOrElse(0)
//  }
//
//  def getFloat(name: String): Float = {
//    p.find(_.getName == name).map(_.getValue).map(_.toFloat).getOrElse(0.0f)
//  }
//
//  def getDouble(name: String): Double = {
//    p.find(_.getName == name).map(_.getValue).map(_.toDouble).getOrElse(0.0)
//  }
//
//  def getChar(name: String): Char = {
//    val c = p.find(_.getName == name).map(_.getValue).getOrElse("").toString().toList
//    c(0)
//  }
//
//  def getString(name: String): String = {
//    p.find(_.getName == name).map(_.getValue).getOrElse("")
//  }
//
//  def getMatrixHandle(name: String): MatrixHandle = {
//    new MatrixHandle(p.find(_.getName == name).map(_.getValue).map(_.toInt).getOrElse(0))
//  }
}
//
//object Parameters {
//  def apply(): Parameters = new Parameters()
//}