package com.amadeus.dataio.core.transformers

import com.amadeus.dataio.core.Logging
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col

trait Repartitioner extends Logging {
  val repartitionNum: Option[Int]
  val repartitionExprs: Option[String]

  def applyRepartition[T](ds: Dataset[T]): Dataset[T] = {
    (repartitionExprs, repartitionNum) match {
      case (Some(exprs), Some(num)) =>
        logger.info(s"repartition: num=$num, exprs=$exprs")
        ds.repartition(num, col(exprs))
      case (None, Some(num)) =>
        logger.info(s"repartition: $num")
        ds.repartition(num)
      case (Some(exprs), None) =>
        logger.info(s"repartition: $exprs")
        ds.repartition(col(exprs))
      case (None, None) => ds
    }
  }
}

object Repartitioner {
  def apply[T](number: Option[Int], column: Option[String]): Dataset[T] => Dataset[T] = {
    new Repartitioner {
      override val repartitionNum: Option[Int]      = number
      override val repartitionExprs: Option[String] = column
    }.applyRepartition
  }
}
