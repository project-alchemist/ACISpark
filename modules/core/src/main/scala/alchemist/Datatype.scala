package alchemist

//sealed abstract class Datatype(val code: Byte, val label: String)
//
//case object None extends Datatype(0, "NONE")
//
//case object ByteType extends Datatype(33, "BYTE")
//case object ShortType extends Datatype(34, "SHORT")
//case object IntType extends Datatype(35, "INT")
//case object LongType extends Datatype(36, "LONG")
//
//case object FloatType extends Datatype(15, "FLOAT")
//case object DoubleType extends Datatype(16, "DOUBLE")
//
//case object CharType extends Datatype(5, "CHAR")
//case object StringType extends Datatype(47, "STRING")
//
//case object CommandCode extends Datatype(48, "COMMAND CODE")
//case object Parameter extends Datatype(49, "PARAMETER")
//case object LibraryID extends Datatype(50, "LIBRARY ID")
//case object MatrixID extends Datatype(51, "MATRIX ID")
//case object MatrixInfo extends Datatype(52, "MATRIX INFO")
//case object DistMatrix extends Datatype(53, "DIST MATRIX")

import enumeratum.values.{ByteEnum, ByteEnumEntry}
import enumeratum.EnumEntry.UpperSnakecase

private[alchemist] sealed abstract class Datatype(override val value: Byte, val label: String)
    extends ByteEnumEntry
    with UpperSnakecase

private[alchemist] object Datatype extends ByteEnum[Datatype] {

  override val values: scala.collection.immutable.IndexedSeq[Datatype] = findValues

  final case object None extends Datatype(0, "NONE")

  final case object Byte  extends Datatype(33, "BYTE")
  final case object Short extends Datatype(34, "SHORT")
  final case object Int   extends Datatype(35, "INT")
  final case object Long  extends Datatype(36, "LONG")

  final case object Float  extends Datatype(15, "FLOAT")
  final case object Double extends Datatype(16, "DOUBLE")

  final case object Char   extends Datatype(1, "CHAR")
  final case object String extends Datatype(46, "STRING")

  final case object CommandCode extends Datatype(48, "COMMAND CODE")
  final case object LibraryID   extends Datatype(49, "LIBRARY ID")
  final case object GroupID     extends Datatype(50, "GROUP ID")
  final case object WorkerID    extends Datatype(51, "WORKER ID")
  final case object WorkerInfo  extends Datatype(52, "WORKER INFO")
  final case object MatrixID    extends Datatype(53, "MATRIX ID")
  final case object MatrixInfo  extends Datatype(54, "MATRIX INFO")
  final case object MatrixBlock extends Datatype(55, "MATRIX BLOCK")
  final case object IndexedRow  extends Datatype(56, "INDEXED ROW")

  final case object Parameter extends Datatype(100, "PARAMETER")
}
