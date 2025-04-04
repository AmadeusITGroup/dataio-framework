package com.amadeus.dataio.config.fields

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RepartitionConfiguratorTest extends AnyFlatSpec with Matchers {
  behavior of "getRepartitionNum"
  "getRepartitionNum" should "return number when it exists in config" in {
    val configStr =
      """
        repartition.num = 42
      """
    implicit val config: Config = ConfigFactory.parseString(configStr)

    val result = getRepartitionNum
    result should be(Some(42))
  }

  it should "return None when number doesn't exist in config" in {
    val configStr =
      """
        some_other_config = "value"
      """
    implicit val config: Config = ConfigFactory.parseString(configStr)

    val result = getRepartitionNum
    result should be(None)
  }

  behavior of "getRepartitionExprs"
  it should "return expressions when they exist in config" in {
    val configStr =
      """
        repartition.exprs = "id, timestamp"
      """
    implicit val config: Config = ConfigFactory.parseString(configStr)

    val result = getRepartitionExprs
    result should be(Some("id, timestamp"))
  }

  it should "return None when expressions don't exist in config" in {
    val configStr =
      """
        some_other_config = "value"
      """
    implicit val config: Config = ConfigFactory.parseString(configStr)

    val result = getRepartitionExprs
    result should be(None)
  }
}
