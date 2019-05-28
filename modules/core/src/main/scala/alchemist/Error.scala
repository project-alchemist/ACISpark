package alchemist

import alchemist.Command.findValues
import enumeratum.EnumEntry.UpperSnakecase
import enumeratum.values.{ ByteEnum, ByteEnumEntry }

private[alchemist] sealed abstract class Error(override val value: Byte, val label: String)
    extends ByteEnumEntry
    with UpperSnakecase

private[alchemist] object Error extends ByteEnum[Error] {

  override val values: scala.collection.immutable.IndexedSeq[Error] = findValues

  final case object None extends Error(0, "NONE")

  final case object InvalidHandshake      extends Error(1, "INVALID HANDSHAKE")
  final case object InvalidClientID       extends Error(2, "INVALID CLIENT ID")
  final case object InvalidSessionID      extends Error(3, "INVALID SESSION ID")
  final case object InconsistentDatatypes extends Error(4, "INCONSISTENT DATATYPES")
}
