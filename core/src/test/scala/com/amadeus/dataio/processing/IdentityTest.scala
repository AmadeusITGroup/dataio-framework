package com.amadeus.dataio.processing

import com.amadeus.dataio.testutils.SparkSuite
import org.apache.spark.sql.DataFrame
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class IdentityTest extends AnyWordSpec with SparkSuite with Matchers {

  val inputData = Seq(
    ("hello", 42),
    ("there", 55)
  )
  lazy val inputColumns: Seq[String] = Seq("text", "number")

  "the Identity processor" should {
    "output the same schema as the input" in {
      val inputDF: DataFrame = sparkSession
        .createDataFrame(
          sparkSession.sparkContext.parallelize(inputData)
        )
        .toDF(
          inputColumns: _*
        )

      val outputDF: DataFrame = new Identity().featurize(inputDF)

      outputDF.schema shouldBe inputDF.schema
    }

    "output the same rows as the input" in {
      val inputDF: DataFrame = sparkSession
        .createDataFrame(
          sparkSession.sparkContext.parallelize(inputData)
        )
        .toDF(
          inputColumns: _*
        )

      val outputDF: DataFrame = new Identity().featurize(inputDF)

      outputDF.collect should contain theSameElementsAs inputDF.collect
    }
  }

  override def getTestName: String = "IdentityTransformerTest"
}
