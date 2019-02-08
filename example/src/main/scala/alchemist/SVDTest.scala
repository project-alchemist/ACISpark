package alchemist

// spark-core
import org.apache.spark.rdd._
// spark-sql
import org.apache.spark.sql.SparkSession
// spark-mllib
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}
import org.apache.spark.mllib.linalg.{DenseVector, Matrices, Matrix, SingularValueDecomposition, Vector, Vectors}

//import alchemist.{AlchemistSession, MatrixHandle}

object SVDTest {

  def run(args: Array[String] = Array.empty[String]): Unit = {
    // Parse parameters from command line arguments
    val k: Int = if (args.length > 0) args(0).toInt else 20
    val infile: String = if (args.length > 1) args(1).toString else ""
    // Print Info
    println("Settings: ")
    println(s"  Target dimension: ${k.toString}")
    if (infile.length > 0)
      println(s"  Input data file: ${infile}")
    println(" ")

    // Launch Spark session
    val startTime = System.nanoTime()
    val spark = SparkSession
      .builder()
      .appName("Alchemist SVD Test")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    println(s"Time cost of starting Spark session: ${(System.nanoTime() - startTime) * 1.0E-9}")
    println(" ")

    val data: IndexedRowMatrix = if (infile.length > 0) loadData(spark, infile) else randomData(spark, 100, 50)

    // Print info
    println(s"spark.conf.getAll: ${spark.conf.getAll.foreach(println)}")
    println(" ")
    println(s"Number of partitions: ${data.rows.getNumPartitions}")
    println(s"getExecutorMemoryStatus: ${sc.getExecutorMemoryStatus.toString()}")
    println(" ")

    println("============================== Testing Spark ==============================")
    testSpark(spark, data, k)

    println("============================ Testing Alchemist ============================")
    testAlchemist(spark, data, k)

    spark.stop
  }

  def testSpark(spark: SparkSession, A: IndexedRowMatrix, k: Int): Unit = {

    val sc = spark.sparkContext

    // Compute the Squared Frobenius Norm
    val sqFroNorm: Double = A.rows.map(row => Vectors.norm(row.vector, 2))
      .map(norm => norm * norm)
      .reduce((a, b) => a + b)

    // Spark built-in truncated SVD
    val startTime = System.nanoTime()
    val svd: SingularValueDecomposition[IndexedRowMatrix, Matrix] = A.computeSVD(k)
//    val mat: RowMatrix = new RowMatrix(data.map(pair => pair._2))
//    val svd: SingularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(k, computeU = false)
//    val v: Matrix = svd.V
    println(s"Time cost of Spark truncated SVD clustering: ${(System.nanoTime() - startTime) * 1.0E-9}")
    println(" ")

    // Compute approximation error
    val vBroadcast = sc.broadcast(svd.V)
    val err: Double = A.rows
      .map(row => (row.vector, vBroadcast.value.transpose.multiply(row.vector)))
      .map(pair => (pair._1, Vectors.dense(vBroadcast.value.multiply(pair._2).toArray)))
      .map(pair => Vectors.sqdist(pair._1, pair._2))
      .reduce((a, b) => a + b)
    val relativeError = err / sqFroNorm
    println("Squared Frobenius error of rank " + k.toString + " SVD is " + err.toString)
    println("Squared Frobenius norm of A is " + sqFroNorm.toString)
    println("Relative Error is " + relativeError.toString)
  }

  def testAlchemist(spark: SparkSession, A: IndexedRowMatrix, k: Int): Unit = {

    val sc = spark.sparkContext
    // Compute the squared Frobenius norm
    val sqFroNorm: Double = A.rows.map(row => Vectors.norm(row.vector, 2))
      .map(norm => norm * norm)
      .reduce((a, b) => a + b)

    // Launch Alchemist
    var startTime = System.nanoTime()
    val als = AlchemistSession.initialize(spark).connect("0.0.0.0", 24960).requestWorkers(2)
    println(s"Time cost of starting Alchemist session: ${(System.nanoTime() - startTime) * 1.0E-9}")
    println(" ")
//
////    // Convert data to indexed vectors and labels
////    startTime = System.nanoTime()
////    val (sortedLabels, indexedMat) = splitLabelVec(data)
////    println(s"Time cost of creating indexed vectors and labels: ${(System.nanoTime() - startTime) * 1.0E-9}")
////    println(" ")
//
//    // Convert Spark IndexedRowMatrix to Alchemist matrix
//    startTime = System.nanoTime()
//    val Ah: MatrixHandle = als.sendIndexedRowMatrix(A)
//    println(s"Time cost of converting Spark matrix to Alchemist matrix: ${(System.nanoTime() - startTime) * 1.0E-9}")
//    println(" ")
//
//    val testLib = als.loadLibrary("TestLib")
//
//    // Alchemist truncated SVD
//    startTime = System.nanoTime()
//    val (Uh, Sh, Vh): (MatrixHandle, MatrixHandle, MatrixHandle) = als.runTask(testLib, "truncatedSVD", Ah, k)
//    println(s"Time cost of Alchemist truncates SVD: ${(System.nanoTime() - startTime) * 1.0E-9}")
//    println(" ")
//
//    // Alchemist matrix to local matrix
//    startTime = System.nanoTime()
//    val V: Array[Array[Double]] = als.getIndexedRowMatrix(Vh).rows.map(row => row.vector.toArray).collect
//    val d = V.size
//    val matV: Matrix = Matrices.dense(k, d, V.flatten)
//    println(s"Time cost of Alchemist matrix to local matrix: ${(System.nanoTime() - startTime) * 1.0E-9}")
//    println(" ")
//    //println("Number of rows of V: " + matV.numRows.toString)
//    //println("Number of columns of V: " + matV.numCols.toString)
//    //println(" ")
//
//    // Compute approximation error
//    val vBroadcast = sc.broadcast(matV)
//    val err: Double = A.rows
//      .map(row => (row.vector, vBroadcast.value.multiply(row.vector)))
//      .map(pair => (pair._1, Vectors.dense(vBroadcast.value.transpose.multiply(pair._2).toArray)))
//      .map(pair => Vectors.sqdist(pair._1, pair._2))
//      .reduce((a, b) => a + b)
//    val relativeError = err / sqFroNorm
//    println("Squared Frobenius error of rank " + k.toString + " SVD is " + err.toString)
//    println("Squared Frobenius norm of A is " + sqFroNorm.toString)
//    println("Relative Error is " + relativeError.toString)

    als.stop
  }

  def randomData(spark: SparkSession, numRows: Long, numCols: Long): IndexedRowMatrix = {
    // Generate random dataset
    val sc = spark.sparkContext
    val r = new scala.util.Random(1000L)

    val startTime = System.nanoTime()

    val indexedRows: RDD[IndexedRow] = sc.parallelize((0L to numRows - 1)
      .map(x => new IndexedRow(x, new DenseVector(Array.fill(numCols.toInt)(r.nextDouble())))))

    val data = new IndexedRowMatrix(indexedRows)

    println(s"Time to generate data: ${(System.nanoTime() - startTime) * 1.0E-9}")
    println(" ")

    data
  }

  def loadData(spark: SparkSession, infile: String): IndexedRowMatrix = {
    // Load and parse data
    val startTime = System.nanoTime()
    val df = spark.read.format("libsvm").load(infile)
    val rawData: RDD[(Int, Vector)] = df.rdd
      .map(pair => (pair(0).toString.toFloat.toInt, Vectors.parse(pair(1).toString)))
      .persist()

//    // Convert data to indexed vectors and labels
//    val sortedLabels = rawData.map(pair => (pair._2, pair._1))
//                           .collect
//                           .sortWith(_._1 < _._1)
//                           .map(pair => pair._2)

    val indexedRows = rawData.map(pair => new IndexedRow(pair._1, new DenseVector(pair._2.toArray)))
    val data = new IndexedRowMatrix(indexedRows)

    println(s"Time to load data: ${(System.nanoTime() - startTime) * 1.0E-9}")
    println(" ")

    data
  }
}
