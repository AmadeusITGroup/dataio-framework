package com.amadeus.dataio.core

import com.amadeus.dataio.config.ConfigNode
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * The parent type of all inputs, that is to say reading in batches and streaming.<br/>
 */
trait Input extends ConfigurableEntity {

  /**
   * Reads data from this input.
   * @param spark The SparkSession which will be used to read the data.
   * @return The data that was read.
   */
  def read(implicit spark: SparkSession): DataFrame
}

object Input {

  /**
   * <p>Creates an instance of an Input subclass.</p>
   * @param config The configuration of the Input.
   * @return An instance of the Input subclass provided in the config argument.
   */
  def apply(config: ConfigNode): Input = InstantiationHelper.instantiateWithCompanionObject[Input](config)
}
