package com.amadeus.dataio.pipes.snowflake

import com.amadeus.dataio.pipes.snowflake.batch.SnowflakeOutput
import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
class SnowflakeOutputTest extends AnyWordSpec with Matchers{

  "SnowflakeOutput" should {
    "be initialized according to configuration" in {

      val config = ConfigFactory.parseMap(
        Map(
          "Output"      -> Map(
            "Name"      -> "my-test-snowflake",
            "Type"      -> "com.amadeus.dataio.pipes.snowflake.batch.SnowflakeOutput",
            "Database"  -> "TESTDATABASE",
            "Schema"    -> "TESTSHEMA",
            "User"      -> "TESTUSER",
            "Url"       -> "yc55220.west-europe.azure.snowflakecomputing.com",
            "Role"      -> "TESTROLE",
            "Warehouse" -> "TESTWAREHOUSE",
            "Mode"      -> "append",
            "Key"       -> "TESTKEY",
            "Table"     -> "TESTTABLE"
          )
        )
      )

      val snowflakeOutputObj = SnowflakeOutput.apply(config.getConfig("Output"))

      snowflakeOutputObj.database shouldEqual "TESTDATABASE"

  }
}
