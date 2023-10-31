package com.amadeus.dataio.core.transformers

import com.amadeus.dataio.core.Logging
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col

trait Repartitioner extends Logging {
  val repartitionNumber: Option[Int]
  val repartitionColumn: Option[String]

  def applyRepartition[T](ds: Dataset[T]): Dataset[T] = {
    logger.info(s"Repartition number: $repartitionNumber, using column: $repartitionColumn.")

    (repartitionColumn, repartitionNumber) match {
      case (Some(column), Some(number)) => ds.repartition(number, col(column))
      case (None, Some(number))         => ds.repartition(number)
      case (Some(column), None)         => ds.repartition(col(column))
      case (None, None)                 => ds
    }
  }
}

object Repartitioner {
  def apply[T](number: Option[Int], column: Option[String]): Dataset[T] => Dataset[T] = {
    new Repartitioner {
      override val repartitionNumber: Option[Int]    = number
      override val repartitionColumn: Option[String] = column
    }.applyRepartition
  }
}
