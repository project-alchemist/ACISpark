package alchemist

private[alchemist] sealed abstract class ParameterValue

private[alchemist] case class Parameter[T](name: String, value: T) extends ParameterValue {

  def toString(withType: Boolean = false): String = {

    var datatype: String = ""
    if (withType) {
      datatype = value match {
        case b: Byte => " (BYTE)"
        case s: Short => " (SHORT)"
        case i: Int => " (INT)"
        case l: Long => " (LONG)"
        case f: Float => " (FLOAT)"
        case d: Double => " (DOUBLE)"
        case c: Char => " (CHAR)"
        case a: String => " (STRING)"
        case o: MatrixID => " (MATRIX ID)"
        case h: MatrixHandle => " (MATRIX INFO)"
      }
    }
    f"${name.concat(s"${datatype}:")}%-16s ${value}"
  }
}

class Parameters {

  var ps: Map[String, ParameterValue] = Map.empty[String, ParameterValue]

  def list(preamble: String = "    ", withType: Boolean = false): this.type = {
    ps foreach { case (_, p) => println(s"${preamble}${p match {
        case Parameter(n: String, v: Byte) => p.asInstanceOf[Parameter[Byte]].toString(withType)
        case Parameter(n: String, v: Short) => p.asInstanceOf[Parameter[Short]].toString(withType)
        case Parameter(n: String, v: Int) => p.asInstanceOf[Parameter[Int]].toString(withType)
        case Parameter(n: String, v: Long) => p.asInstanceOf[Parameter[Long]].toString(withType)
        case Parameter(n: String, v: Float) => p.asInstanceOf[Parameter[Float]].toString(withType)
        case Parameter(n: String, v: Double) => p.asInstanceOf[Parameter[Double]].toString(withType)
        case Parameter(n: String, v: Char) => p.asInstanceOf[Parameter[Char]].toString(withType)
        case Parameter(n: String, v: String) => p.asInstanceOf[Parameter[String]].toString(withType)
        case Parameter(n: String, v: MatrixID) => p.asInstanceOf[Parameter[MatrixID]].toString(withType)
        case Parameter(n: String, v: MatrixHandle) => p.asInstanceOf[Parameter[MatrixHandle]].toString(withType)
        case _ => "UNKNOWN PARAMETER"
      }}")
    }

    this
  }

  def getParameters: Map[String, ParameterValue] = ps

  def put[T](name: String, value: T): this.type = {
    ps = ps + (name -> Parameter[T](name, value))
    this
  }

  def add[T](name: String, value: T): this.type = {
    ps = ps + (name -> Parameter[T](name, value))
    this
  }

  def add[T](p: Parameter[T]): this.type = {
    ps = ps + (p.name -> p)
    this
  }

  def add(p: ParameterValue): this.type = {
    p match {
      case Parameter(n: String, v: Byte) => ps = ps + (p.asInstanceOf[Parameter[Byte]].name -> p)
      case Parameter(n: String, v: Short) => ps = ps + (p.asInstanceOf[Parameter[Short]].name -> p)
      case Parameter(n: String, v: Int) => ps = ps + (p.asInstanceOf[Parameter[Int]].name -> p)
      case Parameter(n: String, v: Long) => ps = ps + (p.asInstanceOf[Parameter[Long]].name -> p)
      case Parameter(n: String, v: Float) => ps = ps + (p.asInstanceOf[Parameter[Float]].name -> p)
      case Parameter(n: String, v: Double) => ps = ps + (p.asInstanceOf[Parameter[Double]].name -> p)
      case Parameter(n: String, v: Char) => ps = ps + (p.asInstanceOf[Parameter[Char]].name -> p)
      case Parameter(n: String, v: String) => ps = ps + (p.asInstanceOf[Parameter[String]].name -> p)
      case Parameter(n: String, v: MatrixID) => ps = ps + (p.asInstanceOf[Parameter[MatrixID]].name -> p)
      case Parameter(n: String, v: MatrixHandle) => ps = ps + (p.asInstanceOf[Parameter[MatrixHandle]].name -> p)
      case _ => ps = ps + ("UNKNOWN TYPE" -> p)
    }
    this
  }

  def get[T](name: String): T = ps(name).asInstanceOf[Parameter[T]].value

  def read[T](name: String): T = ps(name).asInstanceOf[Parameter[T]].value

}
