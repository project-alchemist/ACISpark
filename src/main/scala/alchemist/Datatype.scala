package alchemist

import enumeratum.values.{ByteEnum, ByteEnumEntry}
import enumeratum.EnumEntry.UpperSnakecase

private[alchemist] sealed abstract class Datatype(override val value: Byte) extends ByteEnumEntry with UpperSnakecase

private[alchemist] object Datatype extends ByteEnum[Datatype] {

  override val values: scala.collection.immutable.IndexedSeq[Datatype] = findValues

  final case object NoneType extends Datatype(0)

  final case object ByteType extends Datatype(18)
  final case object ShortType extends Datatype(34)
  final case object IntType extends Datatype(35)
  final case object LongType extends Datatype(36)

  final case object FloatType extends Datatype(15)
  final case object DoubleType extends Datatype(16)

  final case object CharType extends Datatype(1)
  final case object StringType extends Datatype(46)

  final case object CommandCode extends Datatype(47)
  final case object Library extends Datatype(48)
  final case object Matrix extends Datatype(49)
  final case object MatrixHandle extends Datatype(50)
}
