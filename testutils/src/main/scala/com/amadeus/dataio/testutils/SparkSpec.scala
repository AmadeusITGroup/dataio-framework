package com.amadeus.dataio.testutils

import com.amadeus.dataio.testutils.matchers.SparkMatchers
import org.scalatest.flatspec.AnyFlatSpec

/** A convenient base trait for Spark-based test specifications.
  *
  * This trait combines the essential components needed for writing concise and
  * expressive Spark tests:
  *
  *  - [[AnyFlatSpec]] - ScalaTest's FlatSpec testing style
  *  - [[LocalSpark]] - Automatic SparkSession management
  *  - [[ConfigCreator]] - Dataset-specific test matchers
  *
  * === Example Usage ===
  * {{{
  * class MyJobTest extends SparkSpec {
  *   behavior of "myTransformation"
  *
  *   it should "do something successfully" in sparkTest { spark =>
  *     import spark.implicits._
  *     // assertions...
  *   }
  * }
  * }}}
  */
trait SparkSpec extends AnyFlatSpec with LocalSpark with ConfigCreator with SparkMatchers {}
