package com.amadeus.dataio.core.transformers

import com.amadeus.dataio.core.Logging
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col

trait PartitionsSorter extends Logging {
  val sortWithinPartitionColumns: Seq[String]

  def applySortWithinPartitions[T](ds: Dataset[T]): Dataset[T] = {
    logger.info(s"SortWithinPartition: $sortWithinPartitionColumns.")

    ds.sortWithinPartitions(sortWithinPartitionColumns.map(col): _*)
  }
}

object PartitionsSorter {
  def apply[T](columns: Seq[String]): Dataset[T] => Dataset[T] = {

    new PartitionsSorter {
      val sortWithinPartitionColumns: Seq[String] = columns
    }.applySortWithinPartitions
  }
}
