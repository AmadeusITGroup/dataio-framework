package com.amadeus.dataio.core

import com.typesafe.config.Config

import scala.util.Try

/**
 * Mocks an entity that does nothing.
 */
private case class MockEntity()

private object MockEntity {
  val missingFieldException = new Exception("missing Field1")

  /**
   * Create a MockEntity. Expects a mandatory Field1 parameter.
   * @param config The map of config
   * @return the new MockEntity
   */
  def apply(config: Config): MockEntity = {
    Try { config.getString("Field1") }.getOrElse(throw missingFieldException)

    new MockEntity
  }
}
