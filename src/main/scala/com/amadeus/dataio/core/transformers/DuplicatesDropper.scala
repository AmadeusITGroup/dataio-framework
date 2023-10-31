package com.amadeus.dataio.core.transformers

import com.amadeus.dataio.core.Logging
import org.apache.spark.sql.Dataset

trait DuplicatesDropper extends Logging {
  val dropDuplicatesActive: Option[Boolean]
  val dropDuplicatesColumns: Seq[String]

  def applyDropDuplicates[T](ds: Dataset[T]): Dataset[T] = {
    dropDuplicatesActive match {
      case Some(enabled) =>
        if (enabled) {
          logger.info(s"DropDuplicates: $dropDuplicatesColumns.")
          ds.dropDuplicates(dropDuplicatesColumns)
        } else {
          logger.info(s"DropDuplicates: None.")
          ds
        }
      case None =>
        logger.info(s"DropDuplicates: None.")
        ds
    }
  }
}

object DuplicatesDropper {
  def apply[T](active: Option[Boolean], columns: Seq[String]): Dataset[T] => Dataset[T] = {
    new DuplicatesDropper {
      override val dropDuplicatesActive: Option[Boolean] = active
      override val dropDuplicatesColumns: Seq[String]    = columns
    }.applyDropDuplicates
  }
}
