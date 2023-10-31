package com.amadeus.dataio.core

import com.amadeus.dataio.config.ConfigNode
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * The parent type of all outputs, that is to say writing in batches and streaming.<br/>
 */
trait Output extends ConfigurableEntity {

  /**
   * Writes data to this output.
   * @param data The data to write.
   * @param spark The SparkSession which will be used to write the data.
   */
  def write[T](data: Dataset[T])(implicit spark: SparkSession): Unit
}

object Output {

  /**
   * <p>Creates an instance of an Output subclass.</p>
   * @param config The configuration of the output.
   * @return An instance of the Output subclass provided in the config argument.
   */
  def apply(config: ConfigNode): Output = InstantiationHelper.instantiateWithCompanionObject[Output](config)
}
