package alchemist

import enumeratum.values.{ByteEnum, ByteEnumEntry}
import enumeratum.EnumEntry.UpperSnakecase

private[alchemist] sealed abstract class Command(override val value: Byte) extends ByteEnumEntry with UpperSnakecase

private[alchemist] object Command extends ByteEnum[Command] {

  override val values: scala.collection.immutable.IndexedSeq[Command] = findValues

  final case object Wait extends Command(0)

  // Connection
  final case object Handshake extends Command(1)
  final case object RequestId extends Command(2)
  final case object ClientInfo extends Command(3)
  final case object SendTestString extends Command(4)
  final case object RequestTestString extends Command(5)
  final case object CloseConnection extends Command(6)

  // Workers
  final case object RequestWorkers extends Command(11)
  final case object YieldWorkers extends Command(12)
  final case object SendAssignedWorkersInfo extends Command(13)
  final case object ListAllWorkers extends Command(14)
  final case object ListActiveWorkers extends Command(15)
  final case object ListInactiveWorkers extends Command(16)
  final case object ListAssignedWorkers extends Command(17)

  // Libraries
  final case object ListAvailableLibraries extends Command(21)
  final case object LoadLibrary extends Command(22)
  final case object UnloadLibrary extends Command(23)

  // Matrices
  final case object MatrixInfo extends Command(31)
  final case object MatrixLayout extends Command(32)
  final case object SendMatrixBlocks extends Command(33)
  final case object RequestMatrixBlocks extends Command(34)

  // Tasks
  final case object RunTask extends Command(41)

  // Shutting down
  final case object Shutdown extends Command(99)
}
