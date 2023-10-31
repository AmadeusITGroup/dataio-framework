package com.amadeus.dataio.core.transformers

import com.amadeus.dataio.core.Logging
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col

trait RangeRepartitioner extends Logging {
  val repartitionByRangeNumber: Option[Int]
  val repartitionByRangeColumn: Option[String]

  def applyRepartitionByRange[T](ds: Dataset[T]): Dataset[T] = {
    logger.info(s"RepartitionByRange number: $repartitionByRangeNumber, using column: $repartitionByRangeColumn.")

    (repartitionByRangeColumn, repartitionByRangeNumber) match {
      case (Some(columnName), Some(repartitionNumber)) => ds.repartitionByRange(repartitionNumber, col(columnName))
      case (Some(columnName), None)                    => ds.repartitionByRange(col(columnName))
      case _                                           => ds
    }
  }
}

object RangeRepartitioner {
  def apply[T](number: Option[Int], column: Option[String]): Dataset[T] => Dataset[T] = {
    new RangeRepartitioner {
      override val repartitionByRangeNumber: Option[Int]    = number
      override val repartitionByRangeColumn: Option[String] = column
    }.applyRepartitionByRange
  }
}
