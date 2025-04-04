package com.amadeus.dataio.core.transformers

import com.amadeus.dataio.core.Logging
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col

trait RangeRepartitioner extends Logging {
  val repartitionByRangeNum: Option[Int]
  val repartitionByRangeExprs: Option[String]

  def applyRepartitionByRange[T](ds: Dataset[T]): Dataset[T] = {
    (repartitionByRangeExprs, repartitionByRangeNum) match {
      case (Some(columnName), Some(repartitionNumber)) =>
        logger.info(s"repartition_by_range: num=$repartitionByRangeNum, exprs=$repartitionByRangeExprs")
        ds.repartitionByRange(repartitionNumber, col(columnName))
      case (Some(columnName), None) =>
        logger.info(s"repartition_by_range: $repartitionByRangeExprs")
        ds.repartitionByRange(col(columnName))
      case _ => ds
    }
  }
}

object RangeRepartitioner {
  def apply[T](number: Option[Int], column: Option[String]): Dataset[T] => Dataset[T] = {
    new RangeRepartitioner {
      override val repartitionByRangeNum: Option[Int]      = number
      override val repartitionByRangeExprs: Option[String] = column
    }.applyRepartitionByRange
  }
}
