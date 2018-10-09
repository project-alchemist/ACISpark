package alchemist

class Parameter(val _name: String, val _ctype: String, val _value: String) {
  
//  def getName(): String = _name
//
//  def getType(): String = _ctype
//
//  def getValue(): String = _value
//
//  override def toString(): String = {
//
//    val buf = new StringBuilder
//    buf ++= _name + "("
//
//    	if (_ctype == "int") buf ++= "i"
//    	else if (_ctype == "long") buf ++= "l"
//    	else if (_ctype == "long long") buf ++= "ll"
//    	else if (_ctype == "unsigned") buf ++= "u"
//    	else if (_ctype == "unsigned long") buf ++= "ul"
//    	else if (_ctype == "unsigned long long") buf ++= "ull"
//    	else if (_ctype == "long double") buf ++= "ld"
//    	else if (_ctype == "double") buf ++= "d"
//    	else if (_ctype == "float") buf ++= "f"
//    	else if (_ctype == "bool") buf ++= "b"
//    	else if (_ctype == "char") buf ++= "c"
//    	else if (_ctype == "string") buf ++= "s"
//    	else if (_ctype == "matrix handle") buf ++= "mh"
//
//    buf ++= ")" + _value
//    buf.toString
//  }
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