package com.amadeus.dataio.pipes.kafka.streaming

import com.amadeus.dataio.testutils.SparkSpec
import io.github.embeddedkafka.EmbeddedKafka
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.Trigger

class KafkaOutputTest extends SparkSpec with EmbeddedKafka {
  case class InputSchema(year: Int, city: String)

  behavior of "KafkaOutput.write"

  it should "write to embedded Kafka using Spark Structured Streaming" in sparkTest { implicit spark =>
    import spark.implicits._
    implicit val sqlCtx: SQLContext = spark.sqlContext // Required for MemoryStream

    val topic = "test-output-topic"
    val json = """{"year":2025,"city":"Nice"}"""

    val memoryStream: MemoryStream[String] = MemoryStream[String]
    memoryStream.addData(json)

    val df = memoryStream.toDF().selectExpr("CAST(value AS STRING) as value")

    val output = KafkaOutput(
      name = "test",
      trigger = Some(Trigger.AvailableNow),
      timeout = Some(3000),
      mode = "append",
      options = Map(
        "kafka.bootstrap.servers" -> "localhost:6001",
        "topic" -> topic,
        "checkpointLocation" -> s"file:///tmp/spark-kafka-checkpoint-${System.currentTimeMillis()}"
      )
    )

    withRunningKafka {
      output.write(df)

      val result = consumeFirstStringMessageFrom(topic)
      result shouldBe json
    }
  }
}
