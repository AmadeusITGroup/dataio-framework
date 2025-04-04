package com.amadeus.dataio.pipes.snowflake.batch

import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.typesafe.config.{ConfigException, ConfigFactory}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SnowflakeInputTest extends AnyWordSpec with Matchers {

  "SnowflakeInput" should {
    "be initialized according to configuration" in {
      val config = ConfigFactory.parseMap(
        Map(
          "Input" -> Map(
            "Name" -> "my-test-snowflake",
            "Type" -> "com.amadeus.dataio.pipes.snowflake.batch.SnowflakeInput",
            "Options" -> Map(
              "sfDatabase" -> "TESTDATABASE",
              "sfSchema"   -> "TESTSCHEMA",
              "sfUser"     -> "TESTUSER",
              "sfUrl"      -> "snowflake.url.com",
              "dbtable"    -> "TESTTABLE"
            )
          )
        )
      )

      val snowflakeInputObj = SnowflakeInput.apply(config.getConfig("Input"))

      val expectedSnowflakeOptions = Map(
        "sfDatabase" -> "TESTDATABASE",
        "sfSchema"   -> "TESTSCHEMA",
        "sfUser"     -> "TESTUSER",
        "sfUrl"      -> "snowflake.url.com",
        "dbtable"    -> "TESTTABLE"
      )

      snowflakeInputObj.options shouldEqual expectedSnowflakeOptions
    }
  }

  "be initialized according to configuration" in {
    val config = ConfigFactory.parseMap(
      Map(
        "Input" -> Map(
          "Name" -> "my-test-snowflake",
          "Type" -> "com.amadeus.dataio.pipes.snowflake.batch.SnowflakeInput"
        )
      )
    )

    def snowflakeInputObj = SnowflakeInput.apply(config.getConfig("Input"))

    intercept[ConfigException](snowflakeInputObj)

  }

}
