package com.amadeus.dataio.core.transformers

import com.amadeus.dataio.core.time.DateRange
import com.amadeus.dataio.testutils.SparkSuite
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.col
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class DateFiltererTest extends AnyWordSpec with SparkSuite with Matchers with DateFilterer {

  // TODO: in order to have multiple tests, we have to have multiple classes
  // each class needs to redefine the DateRange and DateColumn
  val dateRange: Option[DateRange] = Some(DateRange("2022-09-19", "+1H"))
  val dateColumn: Option[Column]   = Some(col("date"))

  val inputData: Seq[(String, Int, String)] = Seq(
    ("hello", 42, "2022-09-19"),
    ("there", 55, "2022-09-20"), // wil be filtered out
    ("padulo", 99, "2022-09-21") // will be filtered out
  )
  lazy val inputColumns: Seq[String] = Seq("text", "number", "date")

  "the DateFilterer transformation" should {
    "filter the input" in {
      val inputDF: DataFrame = sparkSession
        .createDataFrame(sparkSession.sparkContext.parallelize(inputData))
        .toDF(inputColumns: _*)

      val outputDF: DataFrame = applyDateFilter(inputDF)

      outputDF.count shouldBe 1
    }
  }

  override def getTestName: String = "DateFiltererTest"
}
