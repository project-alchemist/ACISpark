package alchemist.io

import scala.collection.JavaConverters._
import scala.util.Random
import java.io.{InputStream, DataInputStream => JDataInputStream}
import java.nio.{DoubleBuffer, ByteBuffer}
import java.nio.charset.StandardCharsets
import scala.compat.Platform.EOL

class DataInputStream(istream: InputStream) extends JDataInputStream(istream) {
  def readArrayLength(): Int = {
    val result = readLong()
    assert(result.toInt == result)
    return result.toInt
  }

  def readBuffer(): Array[Byte] = {
    val arrlen = readArrayLength()
    val buf    = new Array[Byte](arrlen)
    read(buf)
    buf
  }

  def readDoubleArray(): Array[Double] = {
    val bufLen = readArrayLength()
    assert(bufLen % 8 == 0)
    val buf = new Array[Double](bufLen / 8)
    for (i <- 0 until bufLen / 8) {
      buf(i) = readDouble()
    }
    return buf
  }

  def readString(): String = {
    new String(readBuffer(), StandardCharsets.US_ASCII)
  }
}
