package alchemist

class AlchemistException(message: String, cause: Throwable) extends Exception(message, cause) {
  
  def this(message: String) = this(message, null)
}