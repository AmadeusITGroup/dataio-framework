package com.amadeus.dataio.core.handler.handlers

import com.amadeus.dataio.config.ConfigNodeCollection
import com.amadeus.dataio.pipes.kafka.streaming.KafkaInput
import com.amadeus.dataio.pipes.storage.streaming.StorageInput
import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class InputHandlerTest extends AnyWordSpec with Matchers {

  "Input handler" should {

    "return my KafkaInput" in {
      val config = ConfigFactory.parseMap(
        Map(
          "Input" -> Map(
            "Type"    -> "com.amadeus.dataio.pipes.kafka.streaming.KafkaInput",
            "Name"    -> "my-test-kafka",
            "Brokers" -> "bktv001:9000, bktv002.amadeus.net:8000",
            "Topic"   -> "test.topic",
            "TrustStorePath" -> "Path",
            "SecretsScope"   -> "Scope",
            "KafkaCertSecret" -> "Secret",
            "Options" -> Map(
              "failOnDataLoss"                       -> "false",
              "maxOffsetsPerTrigger"                 -> "20000000",
              "startingOffsets"                      -> "earliest",
              "\"kafka.security.protocol\""          -> "SASL_PLAINTEXT",
              "\"kafka.sasl.kerberos.service.name\"" -> "kafka"
            )
          )
        )
      )

      val configCollection = ConfigNodeCollection("Input", config)
      val inputHandler     = InputHandler(configCollection)

      val myTestStreamInput = inputHandler.getOne("my-test-kafka").asInstanceOf[KafkaInput]

      myTestStreamInput.brokers shouldEqual "bktv001:9000, bktv002.amadeus.net:8000"
      myTestStreamInput.topic shouldEqual Some("test.topic")
      myTestStreamInput.options shouldEqual Map(
        "failOnDataLoss"                   -> "false",
        "maxOffsetsPerTrigger"             -> "20000000",
        "startingOffsets"                  -> "earliest",
        "kafka.security.protocol"          -> "SASL_PLAINTEXT",
        "kafka.sasl.kerberos.service.name" -> "kafka"
      )
    }

    "return my StorageInput" in {
      val config = ConfigFactory.parseMap(
        Map(
          "Input" -> Map(
            "Type"   -> "com.amadeus.dataio.pipes.storage.streaming.StorageInput",
            "Name"   -> "my-test-stream",
            "Path"   -> "input/fileStreamInputConfigTest.csv",
            "Format" -> "csv",
            "Options" -> Map(
              "delimiter" -> ";"
            )
          )
        )
      )

      val configCollection = ConfigNodeCollection("Input", config)
      val inputHandler     = InputHandler(configCollection)

      val myTestStreamInput = inputHandler.getOne("my-test-stream").asInstanceOf[StorageInput]

      myTestStreamInput.path shouldEqual "input/fileStreamInputConfigTest.csv"
      myTestStreamInput.format.get shouldEqual "csv"
      myTestStreamInput.options shouldEqual Map("delimiter" -> ";")
    }

    "return my StorageInput and my KafkaInput " in {
      val config = ConfigFactory.parseMap(
        Map(
          "Input" -> Seq(
            Map(
              "Type"    -> "com.amadeus.dataio.pipes.kafka.streaming.KafkaInput",
              "Name"    -> "my-test-kafka",
              "Brokers" -> "bktv001:9000, bktv002.amadeus.net:8000",
              "Topic"   -> "test.topic",
              "TrustStorePath" -> "Path",
              "SecretsScope"   -> "Scope",
              "KafkaCertSecret" -> "Secret",
              "Options" -> Map(
                "failOnDataLoss"                       -> "false",
                "maxOffsetsPerTrigger"                 -> "20000000",
                "startingOffsets"                      -> "earliest",
                "\"kafka.security.protocol\""          -> "SASL_PLAINTEXT",
                "\"kafka.sasl.kerberos.service.name\"" -> "kafka"
              )
            ),
            Map(
              "Type"   -> "com.amadeus.dataio.pipes.storage.streaming.StorageInput",
              "Name"   -> "my-test-stream",
              "Path"   -> "input/fileStreamInputConfigTest.csv",
              "Format" -> "csv",
              "Options" -> Map(
                "delimiter" -> ";"
              )
            )
          )
        )
      )

      val configCollection = ConfigNodeCollection("Input", config)
      val inputHandler     = InputHandler(configCollection)

      var foundStorage = false
      var foundKafka   = false

      for (input <- inputHandler.getAll) {
        input match {
          case storage: StorageInput =>
            storage.path shouldEqual "input/fileStreamInputConfigTest.csv"
            storage.format.get shouldEqual "csv"
            storage.options shouldEqual Map("delimiter" -> ";")
            foundStorage = true

          case kafka: KafkaInput =>
            kafka.brokers shouldEqual "bktv001:9000, bktv002.amadeus.net:8000"
            kafka.topic shouldEqual Some("test.topic")
            kafka.options shouldEqual Map(
              "failOnDataLoss"                   -> "false",
              "maxOffsetsPerTrigger"             -> "20000000",
              "startingOffsets"                  -> "earliest",
              "kafka.security.protocol"          -> "SASL_PLAINTEXT",
              "kafka.sasl.kerberos.service.name" -> "kafka"
            )
            foundKafka = true
        }
      }

      foundStorage shouldEqual true
      foundKafka shouldEqual true

    }
  }
}
