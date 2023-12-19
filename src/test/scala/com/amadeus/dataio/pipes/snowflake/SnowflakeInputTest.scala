package com.amadeus.dataio.pipes.snowflake

import com.amadeus.dataio.pipes.snowflake.batch.SnowflakeInput
import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.typesafe.config.{ConfigException, ConfigFactory}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SnowflakeInputTest extends AnyWordSpec with Matchers{

  "SnowflakeInput" should {
    "be initialized according to configuration" in {
      val config = ConfigFactory.parseMap(
        Map(
          "Input" -> Map(
            "Name" -> "my-test-snowflake",
            "Type" -> "com.amadeus.dataio.pipes.snowflake.batch.SnowflakeInput",
            "Database" -> "TESTDATABASE",
            "Schema" -> "TESTSCHEMA",
            "User" -> "TESTUSER",
            "Url" -> "snowflake.url.com",
            "Table" -> "TESTTABLE",
            "Query" -> "SELECT * FROM table"
          )
        )
      )

      val snowflakeInputObj = SnowflakeInput.apply(config.getConfig("Input"))

      snowflakeInputObj.database shouldEqual "TESTDATABASE"
      snowflakeInputObj.schema shouldEqual "TESTSCHEMA"
    }
  }

  "be initialized with all optional properties" in {

    val config = ConfigFactory.parseMap(
      Map(
        "Input" -> Map(
          "Name" -> "my-test-snowflake",
          "Type" -> "com.amadeus.dataio.pipes.snowflake.batch.SnowflakeInput",
          "Database" -> "TESTDATABASE",
          "Schema" -> "TESTSCHEMA",
          "User" -> "TESTUSER",
          "Url" -> "snowflake.url.com",
          "Table" -> "TESTTABLE",
          "Query" -> "SELECT * FROM table",
          "Options" -> Map(
            "pem_private_key" -> "TEST_KEY",
            "sfWarehouse" -> "YMZID",
            "sfRole" -> "APP_OWNER",
            "column_mapping" -> "name"
          )
        )
      )
    )

    val snowflakeInputObj = SnowflakeInput.apply(config.getConfig("Input"))

    snowflakeInputObj.options shouldEqual Map("pem_private_key" -> "TEST_KEY",
      "sfWarehouse" -> "YMZID",
      "sfRole" -> "APP_OWNER",
      "column_mapping" -> "name")
    snowflakeInputObj.schema shouldEqual "TESTSCHEMA"
  }

  "throw an exception for missing attribute" in {
    val config = ConfigFactory.parseMap(
      Map(
        "Input" -> Map(
          "Name" -> "my-test-snowflake",
          "Type" -> "com.amadeus.dataio.pipes.snowflake.batch.SnowflakeInput",
          "Schema" -> "TESTSHEMA",
          "User" -> "TESTUSER",
          "Url" -> "snowflake.url.com",
          "Query" -> "SELECT * FROM table",
          "Options" -> Map(
            "pem_private_key" -> "TEST_KEY",
            "sfWarehouse" -> "YMZID",
            "sfRole" -> "APP_OWNER",
            "column_mapping" -> "name"
          )
        )
      )
    )

    intercept[ConfigException.Missing] {
      SnowflakeInput(config.getConfig("Input"))
    }
  }
}
