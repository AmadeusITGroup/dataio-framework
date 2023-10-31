package com.amadeus.dataio.pipes.reporting

import com.amadeus.dataio.core.{Logging, Output}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.Try

case class ExcelOutput(
    path: String,
    bufferPath: String,
    sheetName: String,
    options: Map[String, String] = Map(),
    dropDuplicatesActive: Option[Boolean],
    dropDuplicatesColumns: Seq[String],
    config: Config = ConfigFactory.empty()
) extends Output
    with Logging {

  /**
   * Writes a data in an Excel file on a distributed filesystem.
   *
   * @param data  The data to write.
   * @param spark The SparkSession which will be used to write the data.
   */
  override def write[T](data: Dataset[T])(implicit spark: SparkSession): Unit = {

    writeBuffer(data)
    val bufferedData = spark.read.format("parquet").load(bufferPath)

    var excelWriter = bufferedData.write
      .format("com.crealytics.spark.excel")
      .option("dataAddress", s"$sheetName!A1")
      .option("dateFormat", "yyyy-mm-dd")
      .option("timestampFormat", "yyyy-mm-dd hh:mm:ss")
      .option("header", "true")

    if (options.nonEmpty) {
      excelWriter = excelWriter.options(options)
    }

    logger.info(s"Writing $path!$sheetName report.")
    excelWriter.mode("append").save(path)
  }

  /**
   * Writes data as a parquet buffer.
   *
   * @param data         The buffered data to write.
   * @param deleteOnExit Whether the buffer should be deleted when the application exits. Default to true
   */
  private def writeBuffer[T](data: Dataset[T], deleteOnExit: Boolean = true)(implicit spark: SparkSession): Unit = {
    data.write
      .format("parquet")
      .mode("overwrite")
      .save(bufferPath)

    if (deleteOnExit) FileSystem.get(spark.sparkContext.hadoopConfiguration).deleteOnExit(new Path(bufferPath))
  }
}

object ExcelOutput {
  import com.amadeus.dataio.config.fields._

  def apply(implicit config: Config): ExcelOutput = {

    val path                  = getPath
    val bufferPath            = config.getString("BufferPath")
    val sheetName             = config.getString("SheetName")
    val options               = Try(getOptions).getOrElse(Map())
    val dropDuplicatesActive  = Try(getDropDuplicatesActive).toOption
    val dropDuplicatesColumns = Try(getDropDuplicatesColumns).getOrElse(Nil)

    ExcelOutput(
      path,
      bufferPath,
      sheetName,
      options,
      dropDuplicatesActive,
      dropDuplicatesColumns
    )
  }
}
