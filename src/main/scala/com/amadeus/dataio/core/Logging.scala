package com.amadeus.dataio.core

import org.apache.logging.log4j.scala.Logger

/**
 * This logging trait allows you to use a logger instance even in distributed applications.
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
  @transient protected lazy val logger: Logger = Logger(getClass)
}
