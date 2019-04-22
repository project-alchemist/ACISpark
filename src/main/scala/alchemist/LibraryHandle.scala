package alchemist

case class LibraryHandle(id: LibraryID = LibraryID(0), name: String = "", path: String = "") {

  def getID: LibraryID = id
  def getName: String = name
  def getPath: String = path

  override def toString: String = s"ID:     {$id}\nName:  {$name}\nPath:  {$path}"
}
