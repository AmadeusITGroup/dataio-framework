package com.amadeus.dataio.core.handler.handlers

import com.amadeus.dataio.config.ConfigNodeCollection
import com.amadeus.dataio.core.{Distributor, Logging}
import com.amadeus.dataio.core.handler.Handler
import org.apache.hadoop.fs.FileSystem

/**
 * <p>The DistributionHandler is meant to serve as gateway to distribute reports to one
 * or several destinations, using a variety of distributors. <br/>
 * It contains the list of distributors and passes the the files paths
 * to them each time the send method is called.</p>
 *
 * <p>Use as follows:
 * {{{
 *   val config: ConfigNodeCollection = (...)
 *   val distributionHandler = DistributionHandler(config)
 *   val fs: FileSystem = (...)
 *   distributionHandler.send("distributor-name", "/path/to/file.csv")(fs)
 * }}}
 * </p>
 */
case class DistributionHandler() extends Handler[Distributor] with Logging {

  /**
   * Sends a file through a defined distributor.
   * @param distributorName The name of the distributor to use.
   * @param filePath The path of the file to send on the File system.
   * @param fs The file system where the file is.
   */
  def send(distributorName: String, filePath: String)(implicit fs: FileSystem): Unit = {
    val distributor = getOne[Distributor](distributorName)

    distributor.send(filePath)
  }
}

object DistributionHandler extends Logging {

  /**
   * Creates a DistributionHandler based on a given configuration.
   * @param distributionConfig The collection of config nodes that will be used to instantiate distributors.
   * @return A new instance of DistributionHandler.
   */
  def apply(distributionConfig: ConfigNodeCollection): DistributionHandler = {
    val configNodes = distributionConfig.nodes
    val handler     = DistributionHandler()
    configNodes.foreach(n => handler.add(n.name, Distributor(n)))

    handler
  }
}
