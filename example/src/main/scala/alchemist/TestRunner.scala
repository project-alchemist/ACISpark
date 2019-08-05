package alchemist

object TestRunner {

  def main(args: Array[String]): Unit = {

    val test: String = if (args.length > 0) args(0).toString.toLowerCase else ""
    val hostname: String = if (args.length > 1) args(1).toString else "localhost"
    val port: Int = if (args.length > 2) args(2).toInt else 24960

    println(s"Alchemist hostname and port number: ${hostname}:${port}")

    if (test == "connection") {
      println("Running connection test")
      ConnectionTest.run(hostname, port, args.drop(3))
    }
    else if (test == "svd") {
      println("Running SVD test")
      SVDTest.run(hostname, port, args.drop(3))
    }
    else {
      println("ERROR: No valid test selected")
    }
  }

}
