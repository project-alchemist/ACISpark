package alchemist


//package object alchemist {
//  type ArrayID = Short
//}

private[alchemist] sealed abstract class ParameterValue

private[alchemist] case class Parameter[T](value: T) extends ParameterValue

class Parameters {

  var ps: Map[String, ParameterValue] = Map.empty[String, ParameterValue]

  def getParameters: Map[String, ParameterValue] = ps

  def add[T](name: String, value: T): this.type = { ps = ps + (name -> Parameter[T](value)); this }

  def read[T](name: String): T = ps.get(name).asInstanceOf[Parameter[T]].value

  def addByte(name: String, value: Byte): this.type = { ps = ps + (name -> Parameter[Byte](value)); this}
  def addShort(name: String, value: Short): this.type = { ps = ps + (name -> Parameter[Short](value)); this}
  def addInt(name: String, value: Int): this.type = { ps = ps + (name -> Parameter[Int](value)); this}
  def addLong(name: String, value: Long): this.type = { ps = ps + (name -> Parameter[Long](value)); this}
  def addFloat(name: String, value: Float): this.type = { ps = ps + (name -> Parameter[Float](value)); this}
  def addDouble(name: String, value: Double): this.type = { ps = ps + (name -> Parameter[Double](value)); this}
  def addChar(name: String, value: Char): this.type = { ps = ps + (name -> Parameter[Char](value)); this}
  def addString(name: String, value: String): this.type = { ps = ps + (name -> Parameter[String](value)); this}
  def addArrayID(name: String, value: ArrayID): this.type = { ps = ps + (name -> Parameter[ArrayID](value)); this}

}

//private[alchemist] case class ByteParameter(name: String, datatype: Datatype, value: Byte) extends Parameter
//private[alchemist] case class ShortParameter(name: String, datatype: Datatype, value: Short) extends Parameter
//private[alchemist] case class IntParameter(name: String, datatype: Datatype, value: Int) extends Parameter
//private[alchemist] case class LongParameter(name: String, datatype: Datatype, value: Long) extends Parameter
//private[alchemist] case class FloatParameter(name: String, datatype: Datatype, value: Float) extends Parameter
//private[alchemist] case class DoubleParameter(name: String, datatype: Datatype, value: Double) extends Parameter
//private[alchemist] case class CharParameter(name: String, datatype: Datatype, value: Char) extends Parameter
//private[alchemist] case class StringParameter(name: String, datatype: Datatype, value: String) extends Parameter
//private[alchemist] case class ArrayIDParameter(name: String, datatype: Datatype, value: Short) extends Parameter
