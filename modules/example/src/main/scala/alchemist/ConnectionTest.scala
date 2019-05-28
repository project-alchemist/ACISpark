package alchemist

//import alchemist.{AlchemistSession}
//import alchemist.AlchemistSession.driver
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.distributed.{ IndexedRow, IndexedRowMatrix }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.Map

object ConnectionTest {

  def run(hostname: String = "localhost", args: Array[String] = Array.empty[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder
      .appName("Alchemist Connection Test")
      .getOrCreate
    val startTime = System.nanoTime()
    val als = AlchemistSession
      .initialize(spark)
      .connect(hostname, 24960)
    println(s"Time cost of starting Alchemist session: ${(System.nanoTime() - startTime) * 1.0e-9}")
    println(" ")

    als.listAllWorkers.listInactiveWorkers.listActiveWorkers.listAssignedWorkers
      .requestWorkers(2)
      .listAllWorkers
      .listInactiveWorkers
      .listActiveWorkers
      .listAssignedWorkers
      .sendTestString()

    val lh =
      als.loadLibrary("TestLib", "/Users/kai/Projects/AlLib/target/testlib.dylib", "libs/testlib-assembly-0.1.jar")

    val inArgs: Parameters = new Parameters
    inArgs.add("rank", 32)
    val outArgs: Parameters = als.runTask(lh, "greet", inArgs)
    println("List of output arguments:")
    outArgs.list("    ", withType = true)
    val new_rank = outArgs.get[Long]("new_rank")
    val v        = outArgs.get[String]("vv")
    println(s"new_rank = ${new_rank}")
    println(s"vv = ${v}")

    val mat: IndexedRowMatrix = randomData(spark, 20, 5)

    val matHandle = als.sendIndexedRowMatrix(mat)

    val matCopy: IndexedRowMatrix = als.getIndexedRowMatrix(matHandle)

    println("\nOriginal IndexedRowMatrix:")
    println("--------------------------")
    als.printIndexedRowMatrix(mat)

    println("\nIndexedRowMatrix returned from Alchemist:")
    println("-----------------------------------------")
    als.printIndexedRowMatrix(matCopy)

    spark.stop
    als.stop

  }

  def randomData(spark: SparkSession, numRows: Long, numCols: Long): IndexedRowMatrix = {
    // Generate random dataset
    val sc = spark.sparkContext
    val r  = new scala.util.Random(1000L)

    val startTime = System.nanoTime()

    val indexedRows: RDD[IndexedRow] = sc.parallelize(
      (0L to numRows - 1)
        .map(x => IndexedRow(x, new DenseVector(Array.fill(numCols.toInt)(r.nextDouble()))))
    )

    val data = new IndexedRowMatrix(indexedRows)

    println(s"Time to generate data: ${(System.nanoTime() - startTime) * 1.0e-9}")
    println(" ")

    data
  }
}
