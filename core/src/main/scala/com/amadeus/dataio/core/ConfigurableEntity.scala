package com.amadeus.dataio.core

import com.typesafe.config.Config

trait ConfigurableEntity {
  val name: String
  val config: Config
}
