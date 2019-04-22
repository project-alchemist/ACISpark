package alchemist

import enumeratum.values.{ByteEnum, ByteEnumEntry}
import enumeratum.EnumEntry.UpperSnakecase

private[alchemist] sealed abstract class Layout(override val value: Byte, val label: String) extends ByteEnumEntry with UpperSnakecase

@SerialVersionUID(17L)
private[alchemist] object Layout extends ByteEnum[Layout] with Serializable {

  override val values: scala.collection.immutable.IndexedSeq[Layout] = findValues

  final case object MC_MR extends Layout(0, "MC/MR")
  final case object MC_STAR extends Layout(1,"MC/STAR")
  final case object MD_STAR extends Layout(2,"MD/STAR")
  final case object MR_MC extends Layout(3,"MR/MC")
  final case object MR_STAR extends Layout(4,"MR/STAR")
  final case object STAR_MC extends Layout(5,"STAR/MC")
  final case object STAR_MD extends Layout(6,"STAR/MD")
  final case object STAR_MR extends Layout(7,"STAR/MR")
  final case object STAR_STAR extends Layout(8, "STAR/STAR")
  final case object STAR_VC extends Layout(9,"STAR/VC")
  final case object STAR_VR extends Layout(10,"STAR/VR")
  final case object VC_STAR extends Layout(11,"VC/STAR")
  final case object VR_STAR extends Layout(12,"VR/STAR")
  final case object CIRC_CIRC extends Layout(13,"CIRC/CIRC")
}



