package com.amadeus.dataio.pipes.kafka

import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import com.amadeus.dataio.pipes.kafka.KafkaConfigurator._

class KafkaConfiguratorTest extends AnyWordSpec with Matchers {
  "getTopic" should {
    "return topic_x" when {
      "given Topic  = topic_x " in {
        val config = ConfigFactory.parseMap(
          Map("Topic" -> "topic_x")
        )
        getTopic(config) shouldBe Some("topic_x")
      }
    }
  }
  "getBroker" should {
    "return br1" when {
      "given Brokers  = br1 " in {
        val config = ConfigFactory.parseMap(
          Map("Brokers" -> "br1")
        )
        getBroker(config) shouldBe "br1"
      }
    }
  }
  "getPattern" should {
    "return pat" when {
      "given Pattern  = pat " in {
        val config = ConfigFactory.parseMap(
          Map("Pattern" -> "pat")
        )
        getPattern(config) shouldBe Some("pat")
      }
    }
  }
  "getAssign" should {
    "return asgn" when {
      "given Assign  = asgn " in {
        val config = ConfigFactory.parseMap(
          Map("Assign" -> "asgn")
        )
        getAssign(config) shouldBe Some("asgn")
      }
    }
  }
}
