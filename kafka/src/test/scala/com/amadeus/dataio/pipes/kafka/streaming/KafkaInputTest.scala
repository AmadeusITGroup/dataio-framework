package com.amadeus.dataio.pipes.kafka.streaming

import com.amadeus.dataio.testutils.SparkSpec
import io.github.embeddedkafka.Codecs.stringSerializer
import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.spark.sql.streaming.Trigger

class KafkaInputTest extends SparkSpec with EmbeddedKafka {
  case class InputSchema(year: Int, city: String)

  behavior of "KafkaInput.read"

  it should "read data from embedded Kafka using Spark Structured Streaming" in sparkTest { implicit spark =>
    val topic = "test-input-topic"
    val jsonInput = """{"year":2025,"city":"Nice"}"""

    withRunningKafka {
      publishToKafka(topic, jsonInput)

      // KafkaInput expects options
      val input = KafkaInput(
        name = "test",
        options = Map(
          "kafka.bootstrap.servers" -> "localhost:6001",
          "subscribe" -> topic,
          "startingOffsets" -> "earliest"
        ),
        repartitionExprs = None,
        repartitionNum = None,
        coalesce = None
      )

      val df = input.read(spark)

      val query = df.writeStream
        .format("memory")
        .queryName("kafka_test")
        .outputMode("append")
        .trigger(Trigger.AvailableNow)
        .start()

      query.processAllAvailable()
      query.awaitTermination()
      query.exception.foreach(throw _)

      val resultDf = spark.sql("select * from kafka_test")
      val resultString = resultDf.selectExpr("CAST(value AS STRING)").collect().map(_.getString(0)).head

      resultDf should notBeEmpty
      resultString should equal(jsonInput)
    }
  }
}
