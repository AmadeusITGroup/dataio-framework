package com.amadeus.dataio.core.transformers

import com.amadeus.dataio.core.Logging
import org.apache.spark.sql.Dataset

trait Coalescer extends Logging {
  val coalesce: Option[Int]

  def applyCoalesce[T](ds: Dataset[T]): Dataset[T] = {
    coalesce match {
      case Some(number) =>
        logger.info(s"coalesce: $coalesce")
        ds.coalesce(number)
      case None => ds
    }
  }
}

object Coalescer {
  def apply[T](coalesce: Option[Int]): Dataset[T] => Dataset[T] = {
    val c = coalesce
    new Coalescer {
      override val coalesce: Option[Int] = c
    }.applyCoalesce
  }
}
