package com.amadeus.dataio.pipes.kafka.streaming

import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class KafkaInputTest extends AnyWordSpec with Matchers {

  "KafkaInput" should {
    "be initialized according to configuration" in {

      val config = ConfigFactory.parseMap(
        Map(
          "Input" -> Map(
            "Type"    -> "com.amadeus.dataio.pipes.kafka.streaming.KafkaInput",
            "Name"    -> "my-test-kafka",
            "Brokers" -> "bktv001:9000, bktv002.amadeus.net:8000",
            "Topic"   -> "test.topic",
            "TrustStorePath" ->  "/dbfs/FileStore/Shared/rcmbi/DEV/security/Truststore.jks",
            "SecretsScope" ->  "rcmbi-app-kv",
            "KafkaCertSecret" ->  "bkps29-kafka-cert-pwd",
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

      val kafkaStreamInput = KafkaInput.apply(config.getConfig("Input"))

      kafkaStreamInput.brokers shouldEqual "bktv001:9000, bktv002.amadeus.net:8000"
      kafkaStreamInput.topic shouldEqual Some("test.topic")
      kafkaStreamInput.pattern shouldBe None
      kafkaStreamInput.assign shouldBe None
      kafkaStreamInput.options shouldEqual Map(
        "failOnDataLoss"                   -> "false",
        "maxOffsetsPerTrigger"             -> "20000000",
        "startingOffsets"                  -> "earliest",
        "kafka.security.protocol"          -> "SASL_PLAINTEXT",
        "kafka.sasl.kerberos.service.name" -> "kafka"
      )
    }

    "be initialized with all optional properties" in {

      val config = ConfigFactory.parseMap(
        Map(
          "Input" -> Map(
            "Type"                 -> "com.amadeus.dataio.pipes.kafka.streaming.KafkaInput",
            "Name"                 -> "my-test-kafka",
            "Brokers"              -> "bktv001:9000, bktv002.amadeus.net:8000",
            "Topic"                -> "test.topic",
            "RepartitioningColumn" -> "repCol",
            "RepartitioningNumber" -> "100",
            "Coalesce"             -> "10",
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

      val kafkaStreamInput = KafkaInput.apply(config.getConfig("Input"))

      kafkaStreamInput.brokers shouldEqual "bktv001:9000, bktv002.amadeus.net:8000"
      kafkaStreamInput.topic shouldEqual Some("test.topic")
      kafkaStreamInput.pattern shouldBe None
      kafkaStreamInput.assign shouldBe None
      kafkaStreamInput.coalesce.get shouldEqual 10
      kafkaStreamInput.repartitionColumn.get shouldEqual "repCol"
      kafkaStreamInput.repartitionNumber.get shouldEqual 100
      kafkaStreamInput.options shouldEqual Map(
        "failOnDataLoss"                   -> "false",
        "maxOffsetsPerTrigger"             -> "20000000",
        "startingOffsets"                  -> "earliest",
        "kafka.security.protocol"          -> "SASL_PLAINTEXT",
        "kafka.sasl.kerberos.service.name" -> "kafka"
      )
    }


    "be reading Pattern, Topic and Assign" in {

      val config = ConfigFactory.parseMap(
        Map(
          "Input" -> Map(
            "Type"                 -> "com.amadeus.dataio.pipes.kafka.streaming.KafkaInput",
            "Name"                 -> "my-test-kafka",
            "Brokers"              -> "bktv001:9000, bktv002.amadeus.net:8000",
            "Topic"                -> "test.topic",
            "TrustStorePath" -> "/dbfs/FileStore/Shared/rcmbi/DEV/security/Truststore.jks",
            "SecretsScope" -> "rcmbi-app-kv",
            "KafkaCertSecret" -> "bkps29-kafka-cert-pwd",
            "Pattern"              -> "test.pattern",
            "Assign"               -> "test.assign",
            "RepartitioningColumn" -> "repCol",
            "RepartitioningNumber" -> "100",
            "Coalesce"             -> "10",
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

      val kafkaStreamInput = KafkaInput.apply(config.getConfig("Input"))

      kafkaStreamInput.brokers shouldEqual "bktv001:9000, bktv002.amadeus.net:8000"
      kafkaStreamInput.topic shouldEqual Some("test.topic")
      kafkaStreamInput.pattern shouldEqual Some("test.pattern")
      kafkaStreamInput.assign shouldEqual Some("test.assign")
      kafkaStreamInput.coalesce.get shouldEqual 10
      kafkaStreamInput.repartitionColumn.get shouldEqual "repCol"
      kafkaStreamInput.repartitionNumber.get shouldEqual 100
      kafkaStreamInput.options shouldEqual Map(
        "failOnDataLoss"                   -> "false",
        "maxOffsetsPerTrigger"             -> "20000000",
        "startingOffsets"                  -> "earliest",
        "kafka.security.protocol"          -> "SASL_PLAINTEXT",
        "kafka.sasl.kerberos.service.name" -> "kafka"
      )


    }
  }
}
