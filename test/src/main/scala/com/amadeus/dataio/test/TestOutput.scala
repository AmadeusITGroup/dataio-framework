package com.amadeus.dataio.test

import com.amadeus.dataio.core.{Logging, Output}
import com.typesafe.config.Config
import org.apache.spark.sql.{Dataset, SparkSession}

/** A test output class that simulates writing data to an output by saving it in memory.
  *
  * This class implements the `Output` trait and overrides the `write` method to save the given dataset
  * into an in-memory data store (`TestDataStore`), rather than writing to an external storage.
  *
  * @param name The name of the output, which is used to identify and store the dataset.
  * @param config The configuration for the output, typically including output-specific settings.
  */
case class TestOutput(
    name: String,
    path: String,
    config: Config
) extends Output
    with Logging {
  override def write[T](data: Dataset[T])(implicit spark: SparkSession): Unit = {
    logger.info(s"writing: $name")
    logger.info(s"path: $path")
    TestDataStore.save(path, data.toDF)
  }
}

object TestOutput {
  import com.amadeus.dataio.config.fields._
  def apply(implicit config: Config): TestOutput = {
    val name = getName match {
      case Some(n) => n
      case _       => throw new Exception("name field is required for TestOutput")
    }

    val path = getPath match {
      case Some(p) => p
      case _       => throw new Exception("path field is required for TestOutput")
    }

    TestOutput(
      name,
      path,
      config = config
    )
  }
}
