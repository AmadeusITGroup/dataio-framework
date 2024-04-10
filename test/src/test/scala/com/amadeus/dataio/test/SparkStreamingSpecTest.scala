package com.amadeus.dataio.test

import org.scalatest.flatspec.AnyFlatSpec

class SparkStreamingSpecTest extends AnyFlatSpec with SparkStreamingSpec {
  override def getTestName: String = "SparkStreamingSpecTest"

  "SparkStreamingSpec" should "enable Spark streaming schema inference" in {
    enableSparkStreamingSchemaInference()
    val schemaInferenceEnabled = spark.conf.get("spark.sql.streaming.schemaInference")
    assert(schemaInferenceEnabled == "true")
  }
}
