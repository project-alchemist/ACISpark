package alchemist.io

import scala.collection.JavaConverters._
import java.io.{ PrintWriter, FileOutputStream, OutputStream, IOException, DataOutputStream => JDataOutputStream }
import java.nio.{ DoubleBuffer, ByteBuffer }
import scala.io.Source
import java.nio.charset.StandardCharsets

class DataOutputStream(ostream: OutputStream) extends JDataOutputStream(ostream) {

  // NB: assumes the input is standard ASCII, could be UTF8 then weird stuff happens
  // could use writeBytes member of JDataOutputStream
  def sendString(value: String) {
    val format = 0 // 0 - ASCII, 1 - UTF8
    try {
      if (format == 0) {
        super.writeLong(value.length())
        super.write(value.getBytes("US-ASCII"), 0, value.length())

      } else {
        super.writeUTF(value)
      }
    } catch {
      case e: IOException => println(s"alchemist.io.DataOutputStream: IO Exception thrown when writing String '$value'");
    }
  }

  override def write(b: Array[Byte], off: Int, len: Int) = {
    super.write(b, off, len)
  }

  def sendDoubleArray(buf: Array[Double]) {
    try {
      super.writeLong(buf.length * 8)
      buf.foreach(super.writeDouble)
    } catch {
      case e: IOException =>
        println(s"alchemist.io.DataOutputStream: IO Exception thrown when writing array of Doubles");
    }
  }

  def sendBoolean(value: Boolean): this.type = {
    try {
      super.writeBoolean(value)
    } catch {
      case e: IOException =>
        println(s"alchemist.io.DataOutputStream: IO Exception thrown when writing Boolean '$value'");
    }
    this
  }

  def sendByte(value: Int): this.type = {
    try {
      super.writeByte(value)
    } catch {
      case e: IOException => println(s"alchemist.io.DataOutputStream: IO Exception thrown when writing Byte '$value'");
    }
    this
  }

  def sendBytes(value: String): this.type = {
    try {
      super.writeBytes(value)
    } catch {
      case e: IOException => println(s"alchemist.io.DataOutputStream: IO Exception thrown when writing Bytes '$value'");
    }
    this
  }

  def sendChar(value: Char): this.type = {
    try {
      super.writeChar(value)
    } catch {
      case e: IOException => println(s"alchemist.io.DataOutputStream: IO Exception thrown when writing Char '$value'");
    }
    this
  }

  def sendChars(value: String): this.type = {
    try {
      super.writeChars(value)
    } catch {
      case e: IOException => println(s"alchemist.io.DataOutputStream: IO Exception thrown when writing Chars '$value'");
    }
    this
  }

  def sendDouble(value: Double): this.type = {
    try {
      super.writeDouble(value)
    } catch {
      case e: IOException => println(s"alchemist.io.DataOutputStream: IO Exception thrown when writing Double '$value'");
    }
    this
  }

  def sendFloat(value: Float): this.type = {
    try {
      super.writeFloat(value)
    } catch {
      case e: IOException => println(s"alchemist.io.DataOutputStream: IO Exception thrown when writing Float '$value'");
    }
    this
  }

  def sendInt(value: Int): this.type = {
    try {
      super.writeInt(value)
    } catch {
      case e: IOException => println(s"alchemist.io.DataOutputStream: IO Exception thrown when writing Int '$value'");
    }
    this
  }

  def sendLong(value: Long): this.type = {
    try {
      super.writeLong(value)
    } catch {
      case e: IOException => println(s"alchemist.io.DataOutputStream: IO Exception thrown when writing Long '$value'");
    }
    this
  }

  def sendShort(value: Short): this.type = {
    try {
      super.writeShort(value)
    } catch {
      case e: IOException => println(s"alchemist.io.DataOutputStream: IO Exception thrown when writing Short '$value'");
    }
    this
  }

  def done(): this.type = {
    try {
      super.flush()
    } catch {
      case e: IOException => println(s"alchemist.io.DataOutputStream: IO Exception thrown when flushing");
    }
    this
  }
}
