package com.amadeus.dataio.core.transformers

import com.amadeus.dataio.core.Logging
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col

trait PartitionsSorter extends Logging {
  val sortWithinPartitionExprs: Seq[String]

  def applySortWithinPartitions[T](ds: Dataset[T]): Dataset[T] = {
    if (!sortWithinPartitionExprs.isEmpty) {
      logger.info(s"sort_within_partitions: $sortWithinPartitionExprs")
      ds.sortWithinPartitions(sortWithinPartitionExprs.map(col): _*)
    } else {
      ds
    }
  }
}

object PartitionsSorter {
  def apply[T](columns: Seq[String]): Dataset[T] => Dataset[T] = {

    new PartitionsSorter {
      val sortWithinPartitionExprs: Seq[String] = columns
    }.applySortWithinPartitions
  }
}
