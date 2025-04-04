package com.amadeus.dataio.testutils

import com.typesafe.config.{Config, ConfigFactory}

trait ConfigCreator {
  def createConfig(configStr: String): Config = {
    ConfigFactory.parseString(configStr)
  }
}
