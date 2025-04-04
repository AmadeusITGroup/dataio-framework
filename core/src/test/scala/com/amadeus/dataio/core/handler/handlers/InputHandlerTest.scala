package com.amadeus.dataio.core.handler.handlers

import com.amadeus.dataio.config.ConfigNodeCollection
import com.amadeus.dataio.pipes.spark.streaming.SparkInput
import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class InputHandlerTest extends AnyWordSpec with Matchers {

  "Input handler" should {
    "return my SparkInput" in {
      val config = ConfigFactory.parseMap(
        Map(
          "input" -> Map(
            "type"   -> "com.amadeus.dataio.pipes.spark.streaming.SparkInput",
            "name"   -> "my-test-stream",
            "path"   -> "input/fileStreamInputConfigTest.csv",
            "format" -> "csv",
            "options" -> Map(
              "delimiter" -> ";"
            )
          )
        )
      )

      val configCollection = ConfigNodeCollection("input", config)
      val inputHandler     = InputHandler(configCollection)

      val myTestStreamInput = inputHandler.getOne("my-test-stream").asInstanceOf[SparkInput]

      myTestStreamInput.path shouldEqual Some("input/fileStreamInputConfigTest.csv")
      myTestStreamInput.format shouldEqual Some("csv")
      myTestStreamInput.options shouldEqual Map("delimiter" -> ";")
    }

    "return my two SparkInput" in {
      val config = ConfigFactory.parseMap(
        Map(
          "input" -> Seq(
            Map(
              "type"   -> "com.amadeus.dataio.pipes.spark.streaming.SparkInput",
              "name"   -> "my-test-stream1",
              "path"   -> "input/fileStreamInputConfigTest1.csv",
              "format" -> "csv",
              "options" -> Map(
                "delimiter" -> ";"
              )
            ),
            Map(
              "type"   -> "com.amadeus.dataio.pipes.spark.streaming.SparkInput",
              "name"   -> "my-test-stream2",
              "path"   -> "input/fileStreamInputConfigTest2.csv",
              "format" -> "csv",
              "options" -> Map(
                "delimiter" -> ";"
              )
            )
          )
        )
      )

      val configCollection = ConfigNodeCollection("input", config)
      val inputHandler     = InputHandler(configCollection)

      val node1 = inputHandler.getOne("my-test-stream1").asInstanceOf[SparkInput]
      node1.path shouldEqual Some("input/fileStreamInputConfigTest1.csv")
      node1.format shouldEqual Some("csv")
      node1.options shouldEqual Map("delimiter" -> ";")

      val node2 = inputHandler.getOne("my-test-stream2").asInstanceOf[SparkInput]
      node2.path shouldEqual Some("input/fileStreamInputConfigTest2.csv")
      node2.format shouldEqual Some("csv")
      node2.options shouldEqual Map("delimiter" -> ";")
    }
  }
}
