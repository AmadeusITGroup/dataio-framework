package com.amadeus.dataio.test

import com.amadeus.dataio.core.{Input, Logging}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

/** A test input class that simulates reading data from an input by fetching it from memory.
  *
  * This class implements the `Input` trait and overrides the `read` method to retrieve a dataset
  * from an in-memory data store (`TestDataStore`), rather than reading it from an external data source.
  * It allows for test-specific data retrieval using a configurable namespace or path.
  *
  * @param name The name of the input, which is used to identify and fetch the dataset.
  * @param path The path or namespace used to isolate and fetch the dataset from the in-memory store.
  * @param config The configuration for the input, typically including input-specific settings.
  */
case class TestInput(
    name: String,
    path: String,
    config: Config
) extends Input
    with Logging {

  override def read(implicit spark: SparkSession): DataFrame = {
    logger.info(s"reading: $name")
    logger.info(s"path: $path")
    TestDataStore.load(path)
  }
}

object TestInput {
  import com.amadeus.dataio.config.fields._

  def apply(implicit config: Config): TestInput = {
    val name = getName match {
      case Some(n) => n
      case _       => throw new Exception("name field is required for TestInput")
    }

    val path = getPath match {
      case Some(p) => p
      case _       => throw new Exception("path field is required for TestInput")
    }

    TestInput(
      name,
      path,
      config = config
    )
  }
}
