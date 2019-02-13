package alchemist

case class InconsistentDatatypeException(message: String)  extends Exception(message)

//class AlchemistException(message: String, cause: Throwable) extends Exception(message, cause) {
//
//  def this(message: String) = this(message, null)
//}