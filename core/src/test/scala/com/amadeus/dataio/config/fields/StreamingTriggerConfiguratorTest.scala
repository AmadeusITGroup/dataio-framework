package com.amadeus.dataio.config.fields

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.Duration

class StreamingTriggerConfiguratorTest extends AnyFlatSpec with Matchers {

  behavior of "getStreamingTrigger"
  "getStreamingTrigger" should "return AvailableNow trigger when configured" in {
    val configStr =
      """
       trigger = "AvailableNow"
     """
    implicit val config: Config = ConfigFactory.parseString(configStr)

    val result = getStreamingTrigger
    result.get shouldEqual Trigger.AvailableNow()
  }

  it should "return Continuous trigger when configured with duration" in {
    val configStr =
      """
       trigger = "Continuous"
       duration = "10 minutes"
     """

    implicit val config: Config = ConfigFactory.parseString(configStr)

    val result = getStreamingTrigger
    result.get shouldEqual Trigger.Continuous(Duration("10 minutes"))
  }

  it should "return ProcessingTime trigger when only duration configured" in {
    val configStr =
      """
       duration = "10 minutes"
     """
    implicit val config: Config = ConfigFactory.parseString(configStr)

    val result = getStreamingTrigger
    result.get shouldEqual Trigger.ProcessingTime(Duration("10 minutes"))
  }

  it should "return None when neither trigger nor duration configured" in {
    val configStr =
      """
       some_other_config = "value"
     """
    implicit val config: Config = ConfigFactory.parseString(configStr)

    val result = getStreamingTrigger
    result should be(None)
  }

  it should "throw IllegalArgumentException for invalid trigger/duration combinations" in {
    val configStr =
      """
       trigger = "InvalidTrigger"
       duration = "10 min"
     """
    implicit val config: Config = ConfigFactory.parseString(configStr)

    an[IllegalArgumentException] should be thrownBy {
      getStreamingTrigger
    }
  }
}
