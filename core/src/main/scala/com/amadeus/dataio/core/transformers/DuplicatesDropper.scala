package com.amadeus.dataio.core.transformers

import com.amadeus.dataio.core.Logging
import org.apache.spark.sql.Dataset

trait DuplicatesDropper extends Logging {
  val dropDuplicates: Boolean
  val dropDuplicatesColumns: Seq[String]

  def applyDropDuplicates[T](ds: Dataset[T]): Dataset[T] = {
    if (dropDuplicates) {
      logger.info(s"drop_duplicates: $dropDuplicatesColumns")
      ds.dropDuplicates(dropDuplicatesColumns)
    } else {
      ds
    }
  }
}

object DuplicatesDropper {
  def apply[T](active: Boolean, columns: Seq[String]): Dataset[T] => Dataset[T] = {
    new DuplicatesDropper {
      override val dropDuplicates: Boolean            = active
      override val dropDuplicatesColumns: Seq[String] = columns
    }.applyDropDuplicates
  }
}
