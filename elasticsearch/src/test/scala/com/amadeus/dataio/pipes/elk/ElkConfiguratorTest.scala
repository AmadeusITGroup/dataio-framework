package com.amadeus.dataio.pipes.elk

import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ElkConfiguratorTest extends AnyWordSpec with Matchers {
  import com.amadeus.dataio.pipes.elk.ElkConfigurator._

  "getIndex" should {
    "return index_x given index = index_x" in {
      val config = ConfigFactory.parseMap(
        Map("index" -> "index_x")
      )
      getIndex(config) shouldEqual "index_x"
    }

    "throw an exception given missing index" in {
      val config = ConfigFactory.parseMap(Map.empty[String, String])
      intercept[Exception] {
        getIndex(config)
      }
    }
  }

  "getDateField" should {
    "return timestamp given date_field = timestamp" in {
      val config = ConfigFactory.parseMap(
        Map("date_field" -> "timestamp")
      )
      getDateField(config) shouldEqual "timestamp"
    }

    "throw an exception given missing date_field" in {
      val config = ConfigFactory.parseMap(Map.empty[String, String])
      intercept[Exception] {
        getDateField(config)
      }
    }
  }

  "getSubIndexDatePattern" should {
    "return yyyy.MM given sub_index_date_pattern = yyyy.MM" in {
      val config = ConfigFactory.parseMap(
        Map("sub_index_date_pattern" -> "yyyy.MM")
      )
      getSubIndexDatePattern(config) shouldEqual Some("yyyy.MM")
    }

    "return None given missing sub_index_date_pattern" in {
      val config = ConfigFactory.parseMap(Map.empty[String, String])
      getSubIndexDatePattern(config) shouldBe None
    }
  }

}
