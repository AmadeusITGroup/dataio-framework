package com.amadeus.dataio.processing

import com.amadeus.dataio.testutils.SparkSpec
import org.apache.spark.sql.DataFrame
import org.scalatest.matchers.should.Matchers

class IdentityTest extends SparkSpec with Matchers {

  val inputData = Seq(
    ("hello", 42),
    ("there", 55)
  )
  lazy val inputColumns: Seq[String] = Seq("text", "number")

  behavior of "Identity"
  it should "output the same schema as the input" in sparkTest { spark =>
    val inputDF: DataFrame = spark
      .createDataFrame(
        spark.sparkContext.parallelize(inputData)
      )
      .toDF(
        inputColumns: _*
      )

    val outputDF: DataFrame = new Identity().featurize(inputDF)

    outputDF.schema shouldBe inputDF.schema
  }

  it should "output the same rows as the input" in sparkTest { spark =>
    val inputDF: DataFrame = spark
      .createDataFrame(
        spark.sparkContext.parallelize(inputData)
      )
      .toDF(
        inputColumns: _*
      )

    val outputDF: DataFrame = new Identity().featurize(inputDF)

    outputDF.collect should contain theSameElementsAs inputDF.collect
  }
}
