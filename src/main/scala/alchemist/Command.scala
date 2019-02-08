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


//sealed abstract class Command(val text: String, val code: Byte)
//
//case object Wait extends Command("WAIT",0)
//case object Handshake extends Command("HANDSHAKE",1)
//case object RequestId extends Command("REQUEST ID",2)
//case object ClientInfo extends Command("CLIENT INFO",3)
//case object SendTestString extends Command("SEND TEST STRING",4)
//case object RequestTestString extends Command("REQUEST TEST STRING",5)
//case object CloseConnection extends Command("CLOSE CONNECTION",6)
//
//case object RequestWorkers extends Command("WAIT",11)
//case object YieldWorkers extends Command("HANDSHAKE",12)
//case object SendAssignedWorkersInfo extends Command("REQUEST ID",13)
//case object ListAllWorkers extends Command("CLIENT INFO",14)
//case object ListActiveWorkers extends Command("SEND TEST STRING",15)
//case object ListInactiveWorkers extends Command("REQUEST TEST STRING",16)
//case object ListAssignedWorkers extends Command("CLOSE CONNECTION",17)
//
//case object ListAvailableLibraries extends Command("WAIT",21)
//case object LoadLibrary extends Command("HANDSHAKE",22)
//case object UnloadLibrary extends Command("REQUEST ID",23)
//
//case object MatrixInfo extends Command("WAIT",31)
//case object MatrixLayout extends Command("HANDSHAKE",32)
//case object SendMatrixBlocks extends Command("REQUEST ID",33)
//case object RequestMatrixBlocks extends Command("REQUEST ID",34)
//
//case object RunTask extends Command("REQUEST ID",41)
//
//case object Shutdown extends Command("REQUEST ID",99)