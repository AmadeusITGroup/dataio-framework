package com.amadeus.dataio.pipes.snowflake.batch

import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.typesafe.config.{ConfigException, ConfigFactory}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec


class SnowflakeOutputTest extends AnyWordSpec with Matchers {

  "SnowflakeOutput" should {
    "be initialized according to configuration" in {

      val config = ConfigFactory.parseMap(
        Map(
          "Output" -> Map(
            "Type"     -> "com.amadeus.dataio.output.streaming.SnowflakeOutput",
            "Name"     -> "my-test-snowflake",
            "Mode"     -> "append",
            "Options"  -> Map(
              "dbTable"    -> "test-table",
              "sfUrl"      -> "http://snowflake.com",
              "sfUser"     -> "my-user",
              "sfDatabase" -> "db",
              "sfSchema"   -> "test-schema",
              "sfWarehouse" -> "warehouse",
              "column_mapping" -> "name"
            )
          )
        )
      )

      val snowflakeStreamOutput = SnowflakeOutput.apply(config.getConfig("Output"))

      val expectedSnowflakeOptions = Map(
        "dbTable"    -> "test-table",
        "sfUrl"      -> "http://snowflake.com",
        "sfUser"     -> "my-user",
        "sfDatabase" -> "db",
        "sfSchema"   -> "test-schema",
        "sfWarehouse" -> "warehouse",
        "column_mapping" -> "name"
      )

      snowflakeStreamOutput.options shouldEqual expectedSnowflakeOptions
      snowflakeStreamOutput.mode shouldEqual "append"
    }

    "be throw an exception when mode is missing" in {

      val config = ConfigFactory.parseMap(
        Map(
          "Output" ->  Map(
            "Type"     -> "com.amadeus.dataio.output.streaming.SnowflakeOutput",
            "Name"     -> "my-test-snowflake",
            "Options"  -> Map(
              "dbTable"    -> "test-table",
              "sfUrl"      -> "http://snowflake.com",
              "sfUser"     -> "my-user",
              "sfDatabase" -> "db",
              "sfSchema"   -> "test-schema",
              "sfWarehouse" -> "warehouse",
              "column_mapping" -> "name"
            )
          )
        )
      )

      def snowflakeStreamOutput = SnowflakeOutput.apply(config.getConfig("Output"))
      intercept[ConfigException](snowflakeStreamOutput)
    }

    "throw an exception when options is missing" in {

      val config = ConfigFactory.parseMap(
        Map(
          "Output" ->  Map(
            "Type"     -> "com.amadeus.dataio.output.streaming.SnowflakeOutput",
            "Name"     -> "my-test-snowflake",
          )
        )
      )

      def snowflakeStreamOutput = SnowflakeOutput.apply(config.getConfig("Output"))
      intercept[ConfigException](snowflakeStreamOutput)
    }


  }

}