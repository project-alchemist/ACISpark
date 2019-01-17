package alchemist

import enumeratum.values.{ByteEnum, ByteEnumEntry}
import enumeratum.EnumEntry.UpperSnakecase

private[alchemist] sealed abstract class Datatype(override val value: Byte) extends ByteEnumEntry with UpperSnakecase

private[alchemist] object Datatype extends ByteEnum[Datatype] {

  override val values: scala.collection.immutable.IndexedSeq[Datatype] = findValues

  final case object NoneType extends Datatype(0)

  final case object ByteType extends Datatype(33)
  final case object ShortType extends Datatype(34)
  final case object IntType extends Datatype(35)
  final case object LongType extends Datatype(36)

  final case object FloatType extends Datatype(15)
  final case object DoubleType extends Datatype(16)

  final case object CharType extends Datatype(5)
  final case object StringType extends Datatype(47)

  final case object CommandCode extends Datatype(48)
  final case object Parameter extends Datatype(49)
  final case object LibraryID extends Datatype(50)
  final case object MatrixID extends Datatype(51)
  final case object MatrixInfo extends Datatype(52)
  final case object DistMatrix extends Datatype(53)
}
