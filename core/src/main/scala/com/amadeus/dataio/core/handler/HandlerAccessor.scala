package com.amadeus.dataio.core.handler

import com.amadeus.dataio.config.PipelineConfig
import com.amadeus.dataio.core.handler.handlers.{InputHandler, OutputHandler}

/**
 * Provides a unified way to access the configured handlers of this application.
 * @param input The InputHandler, to easily read dataframes.
 * @param output The OutputHandler, to easily write datasets.
 */
case class HandlerAccessor(
    input: InputHandler,
    output: OutputHandler
)

object HandlerAccessor {

  /**
   * Creates a handler accessor using a config handler.
   * @param config The configuration of the application.
   * @return A new HandlerAccessor.
   */
  def apply(config: PipelineConfig): HandlerAccessor = {
    HandlerAccessor(
      InputHandler(config.input),
      OutputHandler(config.output)
    )
  }
}
