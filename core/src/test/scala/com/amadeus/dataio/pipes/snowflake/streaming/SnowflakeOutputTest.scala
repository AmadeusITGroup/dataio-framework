package com.amadeus.dataio.pipes.snowflake.streaming

import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.apache.spark.sql.streaming.Trigger

import scala.concurrent.duration.Duration

class SnowflakeOutputTest extends AnyWordSpec with Matchers {

  "SnowflakeOutput" should {
    "be initialized according to configuration" in {

      val config = ConfigFactory.parseMap(
        Map(
          "Output" -> Map(
            "Type"     -> "com.amadeus.dataio.output.streaming.SnowflakeOutput",
            "Name"     -> "my-test-snowflake",
            "Mode"     -> "append",
            "Duration" -> "60 seconds",
            "Timeout"  -> "24",
            "Options" -> Map(
              "dbtable"     -> "test-table",
              "sfUrl"      -> "http://snowflake.com",
              "sfUser"     -> "my-user",
              "sfDatabase" -> "db",
              "sfSchema"   -> "test-schema",
              "sfWarehouse" -> "test-warehouse",
              "sfRole"      -> "tester"
            )
          )
        )
      )

      val snowflakeStreamOutput = SnowflakeOutput.apply(config.getConfig("Output"))

      val expectedSnowflakeOptions = Map(
        "sfUrl"       -> "http://snowflake.com",
        "sfUser"      -> "my-user",
        "sfDatabase"  -> "db",
        "sfSchema"    -> "test-schema",
        "sfWarehouse" -> "test-warehouse",
        "sfRole"      -> "tester",
        "dbtable"     -> "test-table"
      )

      snowflakeStreamOutput.outputName shouldEqual Some("my-test-snowflake")
      snowflakeStreamOutput.options shouldEqual expectedSnowflakeOptions
      snowflakeStreamOutput.mode shouldEqual "append"
      snowflakeStreamOutput.addTimestampOnInsert shouldEqual false
      snowflakeStreamOutput.trigger shouldEqual Some(Trigger.ProcessingTime(Duration("60 seconds")))
      snowflakeStreamOutput.timeout shouldEqual 24 * 60 * 60 * 1000
    }

    "be initialized according to configuration with addTimestampOnInsert to true" in {

      val config = ConfigFactory.parseMap(
        Map(
          "Output" -> Map(
            "Type"                 -> "com.amadeus.dataio.output.streaming.SnowflakeOutput",
            "Name"                 -> "my-test-snowflake",
            "Mode"                 -> "append",
            "Duration"             -> "60 seconds",
            "Timeout"              -> "24",
            "AddTimestampOnInsert" -> true,
            "Options" -> Map(
              "dbtable"     -> "test-table",
              "sfUrl"      -> "http://snowflake.com",
              "sfUser"     -> "my-user",
              "sfDatabase" -> "db",
              "sfSchema"   -> "test-schema",
              "sfWarehouse" -> "test-warehouse",
              "sfRole"      -> "tester"
            )
          )
        )
      )

      val snowflakeStreamOutput = SnowflakeOutput.apply(config.getConfig("Output"))

      val expectedSnowflakeOptions = Map(
        "sfUrl"       -> "http://snowflake.com",
        "sfUser"      -> "my-user",
        "sfDatabase"  -> "db",
        "sfSchema"    -> "test-schema",
        "sfWarehouse" -> "test-warehouse",
        "sfRole"      -> "tester",
        "dbtable"     -> "test-table"
      )

      snowflakeStreamOutput.outputName shouldEqual Some("my-test-snowflake")
      snowflakeStreamOutput.options shouldEqual expectedSnowflakeOptions
      snowflakeStreamOutput.mode shouldEqual "append"
      snowflakeStreamOutput.addTimestampOnInsert shouldEqual true
      snowflakeStreamOutput.trigger shouldEqual Some(Trigger.ProcessingTime(Duration("60 seconds")))
      snowflakeStreamOutput.timeout shouldEqual 24 * 60 * 60 * 1000
    }

    "be initialized according to configuration with addTimestampOnInsert to false" in {

      val config = ConfigFactory.parseMap(
        Map(
          "Output" -> Map(
            "Type"                 -> "com.amadeus.dataio.output.streaming.SnowflakeOutput",
            "Name"                 -> "my-test-snowflake",
            "Mode"                 -> "append",
            "Duration"             -> "60 seconds",
            "Timeout"              -> "24",
            "AddTimestampOnInsert" -> false,
            "Options" -> Map(
              "dbtable"     -> "test-table",
              "sfUrl"      -> "http://snowflake.com",
              "sfUser"     -> "my-user",
              "sfDatabase" -> "db",
              "sfSchema"   -> "test-schema",
              "sfWarehouse" -> "test-warehouse",
              "sfRole"      -> "tester"
            )
          )
        )
      )

      val snowflakeStreamOutput = SnowflakeOutput.apply(config.getConfig("Output"))

      val expectedSnowflakeOptions = Map(
        "dbtable"     -> "test-table",
        "sfUrl"      -> "http://snowflake.com",
        "sfUser"     -> "my-user",
        "sfDatabase" -> "db",
        "sfSchema"   -> "test-schema",
        "sfWarehouse" -> "test-warehouse",
        "sfRole"      -> "tester"
      )

      snowflakeStreamOutput.outputName shouldEqual Some("my-test-snowflake")
      snowflakeStreamOutput.options shouldEqual expectedSnowflakeOptions
      snowflakeStreamOutput.mode shouldEqual "append"
      snowflakeStreamOutput.addTimestampOnInsert shouldEqual false
      snowflakeStreamOutput.trigger shouldEqual Some(Trigger.ProcessingTime(Duration("60 seconds")))
      snowflakeStreamOutput.timeout shouldEqual 24 * 60 * 60 * 1000
    }

  }
}
