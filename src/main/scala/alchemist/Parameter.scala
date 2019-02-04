package alchemist

//class Parameter:
//
//name = []
//datatype = []
//value = []
//
//datatypes = {"BYTE": 33,
//"SHORT": 34,
//"INT": 35,
//"LONG": 36,
//"FLOAT": 15,
//"DOUBLE": 16,
//"CHAR": 1,
//"STRING": 46,
//"MATRIX_INFO": 52}


private[alchemist] abstract class Parameter(val name: String, val ctype: Datatype) {

  def getName(): String = name

  def getDatatype(): Datatype = ctype
}

class ByteParameter(val name: String, val ctype: Datatype, val value: Byte) {

  def getValue(): Byte = value
}

class ShortParameter(val name: String, val ctype: Datatype, val value: Short) {

  def getValue(): Short = value
}

class IntParameter(val name: String, val ctype: Datatype, val value: Int) {

  def getValue(): Int = value
}

class LongParameter(val name: String, val ctype: Datatype, val value: Long) {

  def getValue(): Long = value
}

class FloatParameter(val name: String, val ctype: Datatype, val value: Float) {

  def getValue(): Float = value
}

class DoubleParameter(val name: String, val ctype: Datatype, val value: Double) {

  def getValue(): Double = value
}

class CharParameter(val name: String, val ctype: Datatype, val value: Char) {

  def getValue(): Char = value
}

class StringParameter(val name: String, val ctype: Datatype, val value: String) {

  def getValue(): String = value
}

class MatrixInfoParameter(val name: String, val ctype: Datatype, val value: MatrixHandle) {

  def getValue(): MatrixHandle = value
}