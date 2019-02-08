package alchemist

import alchemist.AlchemistSession.driver
import org.apache.spark.sql.SparkSession

object ConnectionTest {

  def run(args: Array[String] = Array.empty[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Alchemist Connection Test")
      .getOrCreate()
    val startTime = System.nanoTime()
    val als = AlchemistSession
    //      .initialize(spark)
          .connect("0.0.0.0", 24960)
    println(s"Time cost of starting Alchemist session: ${(System.nanoTime() - startTime) * 1.0E-9}")
    println(" ")

    als.listAllWorkers("    ")
      .listInactiveWorkers("    ")
      .listActiveWorkers("    ")
      .listAssignedWorkers("    ")
  }
}
