package alchemist

object TestRunner {

  def main(args: Array[String]): Unit = {

    val test: String     = if (args.length > 0) args(0).toString.toLowerCase else ""
    val hostname: String = if (args.length > 1) args(1).toString else "localhost"

    if (test == "connection") {
      println("Connection test")
      ConnectionTest.run(hostname, args.drop(2))
    } else if (test == "svd") {
      println("SVD test")
      SVDTest.run(hostname, args.drop(2))
    } else {
      println("ERROR: No valid test selected")
    }
  }

}
