package alchemist

private[alchemist] sealed abstract class ParameterValue

private[alchemist] case class Parameter[T](name: String, value: T) extends ParameterValue {

  def toString(withType: Boolean = false): String = {

    var datatype: String = ""
    if (withType) {
      datatype = value match {
        case b: Byte => "BYTE                  "
        case s: Short => "SHORT                 "
        case i: Int => "INT                   "
        case l: Long => "LONG                  "
        case f: Float => "FLOAT                 "
        case d: Double => "DOUBLE                "
        case c: Char => "CHAR                  "
        case a: String => "STRING                "
        case o: ArrayID => "ARRAY ID              "
      }
    }
    f"${datatype}${name.concat(":")}%-16s ${value}"
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
        case Parameter(n: String, v: ArrayID) => p.asInstanceOf[Parameter[ArrayID]].toString(withType)
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

  def get[T](name: String): T = ps(name).asInstanceOf[Parameter[T]].value

  def read[T](name: String): T = ps(name).asInstanceOf[Parameter[T]].value

}
