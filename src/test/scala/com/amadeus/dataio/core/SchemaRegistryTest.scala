package com.amadeus.dataio.core

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SchemaRegistryTest extends AnyWordSpec with Matchers {

  case class Test(column: String)

  "getSchemaOrThrowException" when {

    "given a non existing fully qualified name" should {
      "throw an IllegalArgumentException" in {
        try {
          SchemaRegistry.getSchema("none")

          fail()
        } catch {
          case e: IllegalArgumentException => succeed
        }
      }
    }

    "given a proper fully qualified name" should {
      "return the corresponding StructType" in {
        SchemaRegistry.registerSchema[Test]()

        val result = SchemaRegistry.getSchema("com.amadeus.dataio.core.SchemaRegistryTest.Test")

        val expected = StructType(Seq(StructField("column", StringType)))

        result shouldEqual expected
      }
    }
  }
}
