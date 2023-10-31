package com.amadeus.dataio.core.transformers

import com.amadeus.dataio.core.Logging
import org.apache.spark.sql.Dataset

trait WaterMarker extends Logging {
  val watermarkColumn: Option[String]
  val watermarkDuration: Option[String]

  def applyWaterMark[T](ds: Dataset[T]): Dataset[T] = {
    logger.info(s"Watermark: $watermarkDuration, using column $watermarkColumn.")
    (watermarkColumn, watermarkDuration) match {
      case (Some(column), Some(duration)) => ds.withWatermark(column, duration)
      case _                              => ds
    }
  }
}

object WaterMarker {
  def apply[T](column: Option[String], duration: Option[String]): Dataset[T] => Dataset[T] = {
    new WaterMarker {
      override val watermarkColumn: Option[String]   = column
      override val watermarkDuration: Option[String] = duration
    }.applyWaterMark
  }
}
