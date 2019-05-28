package alchemist

class Overhead(val sendOrReceive: Byte, val numBytes: Long, val transferTime: Long, val serializationTime: Long = 0) {

  def toString(spacing: String = "    "): String = {

    var str = ""

    if (sendOrReceive == 0) {
      str += s"${spacing}Bytes sent:                $numBytes\n"
      str += s"${spacing}Serialization time (ms):   ${(1.0 * serializationTime) / 1000000}\n"
      str += s"${spacing}Transfer time (ms):        ${(1.0 * transferTime) / 1000000}\n"
    } else {
      str += s"${spacing}Bytes received:            $numBytes\n"
      str += s"${spacing}Transfer time (ms):        ${(1.0 * transferTime) / 1000000}\n"
      str += s"${spacing}Deserialization time (ms): ${(1.0 * serializationTime) / 1000000}\n"
    }

    str
  }
}
