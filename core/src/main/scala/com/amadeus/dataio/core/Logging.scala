package com.amadeus.dataio.core

import org.slf4j.{Logger, LoggerFactory}

/** This logging trait allows you to use a logger instance even in distributed applications.
  *
  * You must use this feature when you create an object that is going to be used into a spark streaming application.
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
trait Logging {
  @transient protected lazy val logger: Logger = LoggerFactory.getLogger(getClass)
}
