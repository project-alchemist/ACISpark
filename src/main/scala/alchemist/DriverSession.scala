package alchemist

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.DenseVector
import scala.collection.JavaConverters._
import scala.util.Random
import java.io.{
    PrintWriter, FileOutputStream,
    InputStream, OutputStream,
    DataInputStream => JDataInputStream,
    DataOutputStream => JDataOutputStream
}
import java.nio.{
    DoubleBuffer, ByteBuffer
}
import scala.io.Source
//import java.nio.file.{Paths, Files}
//import java.nio.charset.StandardCharsets
import scala.compat.Platform.EOL

import alchemist._
import alchemist.io._


class DriverSession(val istream: InputStream, val ostream: OutputStream) {

  val input = new DataInputStream(istream)
  val output = new DataOutputStream(ostream)
  
  var workerCount: Int = 0
  var workerIds: Array[WorkerId] = _
  var workerInfo: Array[WorkerInfo] = _

//  def handshake(): this.type = {
//    output.sendInt(0x0)
//    output.sendInt(0xABCD)
//    output.sendInt(0x1)
////    val ar = output.finish()
//    if(input.readInt() != 0xDCBA || input.readInt() != 0x1) {
//      throw new ProtocolException("Corrupted handshake")
//    }
//
//    this
//  }
//
//  def writeTestString(): this.type = {
//    output.sendInt(0x1)
//    output.sendString("This is a test string")
//
//    this
//  }
//
//  def readTestString(): this.type = {
//    output.sendInt(0x2)
//    val testString: String = input.readString()
//    println(s"Alchemist.DriverClient: $testString")
//
//    this
//  }
//
//  def getWorkerInfo(): this.type = {
//    output.sendInt(0x3)
//    workerCount = input.readInt()
////    println(s"Alchemist.DriverClient: Number of workers is $workerCount:")
////
////    for (i <- 1 to workerCount) {
////      val hostname = input.readString()
////      val port = input.readInt()
////      println(s"    $i: $hostname:$port")
////    }
//
//    workerIds = (0 until workerCount).map(new WorkerId(_)).toArray
//    workerInfo = workerIds.map(id => new WorkerInfo(id, input.readString(), input.readInt()))
//
//    this
//  }
//
//  def loadLibrary(name: String, path: String): this.type = {
//    output.sendInt(0x4)
//    val args: String = name + " " + path
//    output.sendString(args)
//    if(input.readInt() != 0x1) {
//      throw new ProtocolException(s"Unable to open library $path")
//    }
//
//    this
//  }
//
//  def runCommand(libraryName: String, funName: String, inputParams: Parameters): Parameters = {
//    output.sendInt(0x5)
//
//    output.sendString(serializeArgs(libraryName, funName, inputParams))
//    if(input.readInt() != 0x1) {
//      throw new ProtocolException(s"Unable to run function $funName in $libraryName")
//    }
//    deserializeArgs(input.readString())
//  }
//
//  // creates a MD, STAR matrix
//  // caveat: assumes there are an INT number of rows due to Java array size limitations
//  def sendNewMatrix(rows: Long, cols: Long): Tuple2[MatrixHandle, Array[WorkerId]] = {
//    output.writeInt(0x6)
//    output.writeLong(rows)
//    output.writeLong(cols)
//    output.flush()
//
//    // get matrix handle
//    if (input.readInt() != 0x1) {
//      throw new ProtocolException()
//    }
//    val handle = new MatrixHandle(input.readInt())
//    System.err.println(s"Got handle: ${handle.id}")
//
//    if (input.readInt() != 0x1) {
//      throw new ProtocolException()
//    }
//    var rowWorkerAssignments : Array[WorkerId] = new Array[WorkerId](rows.toInt)
//    var rawrowWorkerAssignments : Array[Int] = new Array[Int](rows.toInt)
//    // Alchemist returns an array whose ith entry is the world rank of the Alchemist
//    // worker that will take the ith row workerIds on the spark side start at 0, so subtract 1
//    for (i <- 0 until rows.toInt) {
//      rowWorkerAssignments(i) = new WorkerId(input.readInt() - 1)
//      rawrowWorkerAssignments(i) = rowWorkerAssignments(i).id
//    }
//
//    return (handle, rowWorkerAssignments)
//  }
//
//  def sendNewMatrixDone(): Unit = {
//
//    if (input.readInt() != 0x1) {
//      throw new ProtocolException()
//    }
//  }
//
//  def getMatrixDimensions(handle: MatrixHandle): Tuple2[Long, Int] = {
//    output.writeInt(0x7)
//    output.writeInt(handle.id)
//    output.flush()
//    if (input.readInt() != 0x1) {
//      throw new ProtocolException()
//    }
//    return (input.readLong(), input.readLong().toInt)
//  }
//
//  def getTranspose(handle: MatrixHandle): MatrixHandle = {
//    output.writeInt(0x8)
//    output.writeInt(handle.id)
//    output.flush()
//    if (input.readInt() != 0x1) {
//      throw new ProtocolException()
//    }
//    val transposeHandle = new MatrixHandle(input.readInt())
//    System.err.println(s"Got handle: ${transposeHandle.id}")
//    return transposeHandle
//  }
//
//  def matrixMultiply(handleA: MatrixHandle, handleB: MatrixHandle): MatrixHandle = {
//    output.writeInt(0x9)
//    output.writeInt(handleA.id)
//    output.writeInt(handleB.id)
//    output.flush()
//    if (input.readInt() != 0x1) {
//      throw new ProtocolException()
//    }
//    val handle = new MatrixHandle(input.readInt())
//    System.err.println(s"Got handle: ${handle.id}")
//    return handle
//  }
//
//  // layout maps each partition to a worker id (so has length number of partitions in the spark matrix being retrieved)
//  def getIndexedRowMatrix(mat: MatrixHandle, layout: Array[WorkerId]) = {
//    output.writeInt(0x10)
//    output.writeInt(mat.id)
//    output.writeLong(layout.length)
//    layout.map(w => output.writeInt(w.id))
//    output.flush()
//    if (input.readInt() != 0x1) {
//      throw new ProtocolException()
//    }
//  }
//
//  def readHDF5(fname: String, varname: String) : MatrixHandle = {
//    output.writeInt(0x11)
//    output.sendString(fname)
//    output.sendString(varname)
//    output.flush()
//
//    if (input.readInt() != 0x1) {
//      throw new ProtocolException()
//    }
//
//    val matHandle = new MatrixHandle(input.readInt())
//    return matHandle
//  }
//
//  def shutdown(): Unit = {
//    output.writeInt(0xFFFFFFFF)
//    output.flush()
//    if (input.readInt() != 0x1) {
//      throw new ProtocolException("Unable to stop Alchemist")
//    }
//    else {
//      output.writeInt(0x1)
//    }
//  }
//
//  def serializeArgs(libraryName: String, funName: String, inputParams: Parameters): String = {
//    libraryName + " " + funName + " " + inputParams
//  }
//
//  def deserializeArgs(outputArgs: String): Parameters = {
//    val outParameters = Parameters()
//
//    // Split the string the hard way because for some reason 'split' doesn't want to work in my implementation
//    def splitString(s: String): scala.collection.mutable.ArrayBuffer[String] = {
//      var t = s
//      val sBuffer = scala.collection.mutable.ArrayBuffer.empty[String]
//      var done = false
//      while (!done) {
//        sBuffer += t.slice(0, t.indexOf(" "))
//        t = t.slice(t.indexOf(" ")+1, t.length)
//        if (t.indexOf(" ") == -1) done = true
//      }
//      return sBuffer
//    }
//
//    val outputArgsArray = splitString(outputArgs)
//
//    outputArgsArray.foreach(println)
//
//    for (arg <- outputArgsArray) {
//      val argName = arg.slice(0, arg.indexOf("("))
//      val argType = arg.slice(arg.indexOf("(")+1, arg.indexOf(")"))
//      val argValue = arg.slice(arg.indexOf(")")+1, arg.length)
//
//      outParameters.addParameter(argName, argType, argValue)
//    }
//    outParameters
//  }
}