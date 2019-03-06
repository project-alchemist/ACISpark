package alchemist

import enumeratum.values.{ByteEnum, ByteEnumEntry}
import enumeratum.EnumEntry.UpperSnakecase

private[alchemist] sealed abstract class Command(override val value: Byte, val label: String) extends ByteEnumEntry with UpperSnakecase

private[alchemist] object Command extends ByteEnum[Command] {

  override val values: scala.collection.immutable.IndexedSeq[Command] = findValues

  final case object Wait extends Command(0, "WAIT")

  // Connection
  final case object Handshake extends Command(1,"HANDSHAKE")
  final case object RequestId extends Command(2,"REQUEST ID")
  final case object ClientInfo extends Command(3,"CLIENT INFO")
  final case object SendTestString extends Command(4,"SEND TEST STRING")
  final case object RequestTestString extends Command(5,"REQUEST TEST STRING")
  final case object CloseConnection extends Command(6,"CLOSE CONNECTION")

  // Workers
  final case object RequestWorkers extends Command(11,"REQUEST WORKERS")
  final case object YieldWorkers extends Command(12, "YIELD WORKERS")
  final case object SendAssignedWorkersInfo extends Command(13,"SEND ASSIGNED WORKERS INFO")
  final case object ListAllWorkers extends Command(14,"LIST ALL WORKERS")
  final case object ListActiveWorkers extends Command(15,"LIST ACTIVE WORKERS")
  final case object ListInactiveWorkers extends Command(16,"LIST INACTIVE WORKERS")
  final case object ListAssignedWorkers extends Command(17,"LIST ASSIGNED WORKERS")

  // Libraries
  final case object ListAvailableLibraries extends Command(21,"LIST AVAILABLE LIBRARIES")
  final case object LoadLibrary extends Command(22,"LOAD LIBRARY")
  final case object UnloadLibrary extends Command(23,"UNLOAD LIBRARY")

  // Arrays
  final case object ArrayInfo extends Command(31,"ARRAY INFO")
  final case object ArrayLayout extends Command(32,"ARRAY LAYOUT")
  final case object SendIndexedRows extends Command(33, "SEND INDEXED ROWS")
  final case object SendArrayBlocks extends Command(34, "SEND ARRAY BLOCKS")
  final case object RequestIndexedRows extends Command(35,"REQUEST INDEXED ROWS")
  final case object RequestArrayBlocks extends Command(36,"REQUEST ARRAY BLOCKS")

  // Tasks
  final case object RunTask extends Command(41,"RUN TASK")

  // Shutting down
  final case object Shutdown extends Command(99, "SHUTDOWN")
}


