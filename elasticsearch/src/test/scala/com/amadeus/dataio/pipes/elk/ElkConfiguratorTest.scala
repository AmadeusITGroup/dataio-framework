package com.amadeus.dataio.pipes.elk

import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ElkConfiguratorTest extends AnyWordSpec with Matchers {
  import com.amadeus.dataio.pipes.elk.ElkConfigurator._

  "getIndex" should {
    "return index_x given Index = index_x" in {
      val config = ConfigFactory.parseMap(
        Map("Index" -> "index_x")
      )
      getIndex(config) shouldEqual "index_x"
    }

    "throw an exception given missing Index" in {
      val config = ConfigFactory.parseMap(Map.empty[String, String])
      intercept[Exception] {
        getIndex(config)
      }
    }
  }

  "getElkDateField" should {
    "return timestamp given DateField = timestamp" in {
      val config = ConfigFactory.parseMap(
        Map("DateField" -> "timestamp")
      )
      getDateField(config) shouldEqual "timestamp"
    }

    "throw an exception given missing Index" in {
      val config = ConfigFactory.parseMap(Map.empty[String, String])
      intercept[Exception] {
        getDateField(config)
      }
    }
  }

  "getSubIndexDatePattern" should {
    "return yyyy.MM given SubIndexDatePattern = yyyy.MM" in {
      val config = ConfigFactory.parseMap(
        Map("SubIndexDatePattern" -> "yyyy.MM")
      )
      getSubIndexDatePattern(config) shouldEqual Some("yyyy.MM")
    }

    "return None given missing SubIndexDatePattern" in {
      val config = ConfigFactory.parseMap(Map.empty[String, String])
      getSubIndexDatePattern(config) shouldBe None
    }
  }

}
