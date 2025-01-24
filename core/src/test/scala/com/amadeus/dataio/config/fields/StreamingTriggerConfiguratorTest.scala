package com.amadeus.dataio.config.fields

import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.Duration

class StreamingTriggerConfiguratorTest extends AnyWordSpec with Matchers {

  "getStreamingTrigger" should {

    "return AvailableNow trigger given AvailableNow configuration without Duration" in {
      val config = ConfigFactory.parseMap(
        Map("Trigger" -> "AvailableNow")
      )

      val result = getStreamingTrigger(config)
      result shouldEqual Some(Trigger.AvailableNow())
    }

    "return AvailableNow trigger given AvailableNow configuration with Duration" in {
      val config = ConfigFactory.parseMap(
        Map("Trigger" -> "AvailableNow", "Duration" -> "10 minutes")
      )

      val result = getStreamingTrigger(config)
      result shouldEqual Some(Trigger.AvailableNow())
    }

    "return Continuous trigger" in {
      val config = ConfigFactory.parseMap(
        Map("Trigger" -> "Continuous", "Duration" -> "10 minutes")
      )

      val result = getStreamingTrigger(config)
      result shouldEqual Some(Trigger.Continuous(Duration("10 minutes")))
    }

    "return ProcessingTime trigger" in {
      val config = ConfigFactory.parseMap(
        Map("Duration" -> "1 minute")
      )

      val result = getStreamingTrigger(config)
      result shouldEqual Some(Trigger.ProcessingTime(Duration("1 minute")))
    }

    "return none given configuration without Trigger nor Duration" in {
      val config = ConfigFactory.parseMap(
        Map.empty[String, String]
      )

      val result = getStreamingTrigger(config)
      result shouldEqual None
    }

    "throws exception given configuration without proper Trigger nor proper Duration" in {
      intercept[IllegalArgumentException] {
        val config = ConfigFactory.parseMap(
          Map("Trigger" -> "NOT_VALID", "Duration" -> "10 minutes")
        )

        getStreamingTrigger(config)
      }
    }

    "throws exception given configuration without proper Trigger nor Duration" in {
      intercept[IllegalArgumentException] {
        val config = ConfigFactory.parseMap(
          Map("Trigger" -> "NOT_VALID")
        )

        getStreamingTrigger(config)
      }
    }
  }
}
