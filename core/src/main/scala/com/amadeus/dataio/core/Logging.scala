package com.amadeus.dataio.core

import org.slf4j.{Logger, LoggerFactory}

/** Trait providing a lazily initialized `com.amadeus.dataio` logger for use in classes that extend it.
  *
  * Usage example:
  * {{{
  * class MyClass extends Logging {
  *   ...
  *   logger.debug("your message goes here")
  *   logger.info("your message goes here")
  *   logger.warn("your message goes here")
  *   logger.error("your message goes here")
  *   ...
  * }
  * }}}
  */
private[dataio] trait Logging {
  @transient protected lazy val logger: Logger = LoggerFactory.getLogger("com.amadeus.dataio")
}
