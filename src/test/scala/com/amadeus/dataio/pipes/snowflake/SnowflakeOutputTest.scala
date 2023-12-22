package com.amadeus.dataio.pipes.snowflake

import com.amadeus.dataio.pipes.snowflake.batch.SnowflakeOutput
import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.typesafe.config.{ConfigException, ConfigFactory}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
class SnowflakeOutputTest extends AnyWordSpec with Matchers{

  "SnowflakeOutput" should {
    "be initialized according to configuration" in {
      val config = ConfigFactory.parseMap(
        Map(
          "Output" -> Map(
            "Name" -> "my-test-snowflake",
            "Type" -> "com.amadeus.dataio.pipes.snowflake.batch.SnowflakeOutput",
            "Database" -> "TESTDATABASE",
            "Schema"   -> "TESTSCHEMA",
            "User"     -> "TESTUSER",
            "Url"      -> "snowflake.url.com",
            "Mode"     -> "append",
            "Table"    -> "TESTTABLE"
          )
        )
      )

      val snowflakeOutputObj = SnowflakeOutput.apply(config.getConfig("Output"))

      snowflakeOutputObj.database shouldEqual "TESTDATABASE"
      snowflakeOutputObj.schema shouldEqual "TESTSCHEMA"
    }
  }

  "be initialized with all optional properties" in {

    val config = ConfigFactory.parseMap(
      Map(
        "Output" -> Map(
          "Name" -> "my-test-snowflake",
          "Type" -> "com.amadeus.dataio.pipes.snowflake.batch.SnowflakeOutput",
          "Database" -> "TESTDATABASE",
          "Schema"   -> "TESTSCHEMA",
          "User"     -> "TESTUSER",
          "Url"      -> "snowflake.url.com",
          "Mode"     -> "append",
          "Table"    -> "TESTTABLE",
          "Options" -> Map(
            "pem_private_key" -> "TEST_KEY",
            "sfWarehouse"     -> "YMZID",
            "sfRole"          -> "APP_OWNER",
            "column_mapping"  -> "name"
          )
        )
      )
    )

    val snowflakeOutputObj = SnowflakeOutput.apply(config.getConfig("Output"))

    snowflakeOutputObj.options shouldEqual Map("pem_private_key" -> "TEST_KEY",
      "sfWarehouse" -> "YMZID",
      "sfRole" -> "APP_OWNER",
      "column_mapping" -> "name")
    snowflakeOutputObj.schema shouldEqual "TESTSCHEMA"
  }

  "throw an exception for missing attribute" in {
    val config = ConfigFactory.parseMap(
      Map(
        "Output" -> Map(
          "Name"    -> "my-test-snowflake",
          "Type"    -> "com.amadeus.dataio.pipes.snowflake.batch.SnowflakeOutput",
          "Schema"  -> "TESTSHEMA",
          "User"    -> "TESTUSER",
          "Url"     -> "snowflake.url.com",
          "Mode"    -> "append",
          "Options" -> Map(
            "pem_private_key" -> "TEST_KEY",
            "sfWarehouse"     -> "YMZID",
            "sfRole"          -> "APP_OWNER",
            "column_mapping"  -> "name"
          )
        )
      )
    )

    intercept[ConfigException.Missing] {
      SnowflakeOutput(config.getConfig("Output"))
    }
  }
}
