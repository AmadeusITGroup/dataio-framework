package com.amadeus.dataio.core

import com.amadeus.dataio.config.ConfigNode
import org.apache.hadoop.fs.FileSystem

/**
 * Defines the common interface and basic behaviour of the distributors, namely
 * sending files to their respective destinations (email addresses, remote servers, etc.).
 */
trait Distributor extends ConfigurableEntity {

  /**
   * Sends a report through this Distributor.
   * @param path The path of the file  to send.
   * @param fs   The FileSystem on which the file is stored.
   */
  def send(path: String)(implicit fs: FileSystem): Unit

}

object Distributor {

  /**
   * Creates an instance of a Distributor subclass.
   * @param config The configuration of the Distributor.
   * @return An instance of the Distributor subclass provided in the config argument.
   */
  def apply(config: ConfigNode): Distributor = InstantiationHelper.instantiateWithCompanionObject[Distributor](config)
}
