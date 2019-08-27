# ACISpark: Apache Spark Interface for Alchemist

ACISpark is the Apache Spark interface for Alchemist, a HPC interface for data analysis frameworks that can be found at https://github.com/project-alchemist/Alchemist. ACISpark allows Spark users to connect to Alchemist and offload distributed linear algebra operations to more efficent MPI-based implementations.

## Requirements

ACISpark requires Scala (tested with version 2.11.12), Apache Spark (tested with version 2.4.0), sbt (tested with version 1.2.8), and the latest version of Alchemist and the testing library TestLib (https://github.com/project-alchemist/TestLib). Other versions of Scala, Spark and sbt will probably work fine as long as they're not too old. Note that pre-built binaries of Apache Spark 2.4.x, except for 2.4.2, are compiled for Scala 2.11, therefore ACISpark has not been tested with Scala 2.12, but it seems unlikey that ACISpark itself would have issues with it.

## Downloading and building ACISpark

Clone ACISpark from https://github.com/project-alchemist/ACISpark and set the environment variable `ACISPARK_PATH=/path/to/ACISpark/directory`.

ACISpark has a core module (ACISpark) and `example` module that illustrates how it can be used in existing Spark applications. We will build a fat JAR of the `example` module that automatically includes the ACISpark.

1) Go to the ACISPARK_PATH directory
2) Run `sbt` (without arguments)
3) Enter `project alchemist-example`
4) Enter `assembly`

This will create a fat JAR `ACISPARK_PATH/example/target/scala-2.11/alchemist-example-0.5.jar`. For convenience, it may be a good idea to set an environment variable to point to the JAR, for instance `ACISPARK_JAR`.

## Running ACISpark

There are currently two examples to test the implementation of Alchemist and ACISpark, `ConnectionTest` and `SVDTest`. `ConnectionTest` tests the connection to Alchemist and sends a randomly-generated IndexedRowMatrix from ACISpark to Alchemist, which then return the same matrix. `SVDTest` sends a randomly-generated IndexedRowMatrix from ACISpark to Alchemist, which then computes the rank-k truncated SVD of the matrix and returns the singular values and vectors.

Use `spark-submit` to run the tests, for instance

`spark-submit --master local[$1] --class alchemist.TestRunner $ACISPARK_JAR $2 $3 $4 $5 ...`

where `$1` is the number of nodes the user want ACISpark to run on and `$2` is either `connection` for the connection test or `svd` for the SVD test. `$3` is the path of the shared library file (.so on Linux, .dylib on Mac) for the TestLib library. `$4` and `$5` are the hostname and port number that Alchemist is running on; if omitted, they will default to `localhost` and `24960`, respectively, which assumes that Alchemist is running on port 24960 on the same machine as ACISpark. Additional arguments (`$5`, `$6`, etc.) will be forwarded as input parameters to the test, for instance the rank k of the truncated SVD.

See `ACISpark/example/src/main/scala/alchemist/SVDTest.scala` for the truncated SVD test and `ACISpark/example/src/main/scala/alchemist/ConnectionTest.scala` for the connection test.

---------------------------------------------------

Additional example codes and more extensive documentation will be added at a later date.

## To-Do

1) **Support for distributed matrix layouts**. Currently limited to basic layouts. *Expected beginning of September 2019*.
2) **Settings files and more command-line options**. Currently basic settings are sent as command-line arguments. *Expected beginning of September 2019*.
3) **Better error handling**. *Ongoing*

