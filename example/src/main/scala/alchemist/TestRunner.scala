package alchemist

object TestRunner {

  def main(args: Array[String]): Unit = {

    val test: String = if (args.length > 0) args(0).toString.toLowerCase else ""

    if (test == "connection") {
      println("Connection test")
      ConnectionTest.run()
    }
    else if (test == "svd") {
      println("SVD test")
      SVDTest.run(args.drop(1))
    }
    else {
      println("ERROR: No valid test selected")
    }
  }

}
