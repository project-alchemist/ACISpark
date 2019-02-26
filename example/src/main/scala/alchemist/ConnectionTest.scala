package alchemist

//import alchemist.{AlchemistSession}
//import alchemist.AlchemistSession.driver
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

    als.listAllWorkers
      .listInactiveWorkers
      .listActiveWorkers
      .listAssignedWorkers
      .requestWorkers(2)
      .listAllWorkers
      .listInactiveWorkers
      .listActiveWorkers
      .listAssignedWorkers
      .sendTestString

    val lh = als.loadLibrary("TestLib", "/Users/kai/Projects/AlLib/target/testlib.dylib", "libs/testlib-assembly-0.1.jar")

    val inArgs: Parameters = new Parameters
    inArgs.add[Int]("rank", 32)
    als.runTask(lh.id,"greet", inArgs)
  }
}
