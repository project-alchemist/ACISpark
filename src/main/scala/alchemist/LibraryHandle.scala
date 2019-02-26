package alchemist

case class LibraryHandle(id: Byte, name: String, path: String) {

  def getID: Short = id
  def getName: String = name
  def getPath: String = path

  override def toString: String = s"ID:     {$id}\nName:  {$name}\nPath:  {$path}"
}
