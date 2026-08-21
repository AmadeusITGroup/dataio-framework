package com.amadeus.dataio.pipes.elasticsearch

/**
 * Define common functions to be used for ElasticsearchOutput with date sub-indexing.
 */
private[elasticsearch] trait ElasticsearchOutputCommons {

  /** the spark format to use to write to elasticsearch */
  val Format: String = "es"

  /** the Index to write to. */
  def index: String

  /** The date field to use for sub index partitioning. */
  def dateField: String

  /** the date suffix pattern to use for the sub index. */
  def suffixDatePattern: String

  /**
   * compute the  full index name with a date partitioning to optimize Elasticsearch queries performances.
   *
   * @return the full index name with the date partitioning suffix
   */
  def computeFullIndexName(): String = index + s".{$dateField|$suffixDatePattern}"
}

/**
 * Define common static variable to be used for ElasticsearchOutput with date sub-indexing.
 */
private[elasticsearch] object ElasticsearchOutputCommons {

  /** the default date suffix pattern to use for the full index. */
  val DefaultSuffixDatePattern: String = "yyyy.MM"

  /**
   * Check if the nodes to target are defined in the options map
   *
   * @param options the map containing the options
   * @throws IllegalArgumentException in case the nodes are not defined or empty.
   */
  def checkNodesIsDefined(options: Map[String, String]): Unit = {
    options.get("es.nodes") match {
      case Some(nodes) if nodes.nonEmpty =>
      case _                             => throw new IllegalArgumentException("es.nodes must be defined in the options.")
    }
  }

  /**
   * Check if the port to target is defined in the options map
   *
   * @param options the map containing the options
   * @throws IllegalArgumentException in case the port is not defined or empty.
   */
  def checkPortIsDefined(options: Map[String, String]): Unit = {
    options.get("es.port") match {
      case Some(port) if port.nonEmpty =>
      case _                           => throw new IllegalArgumentException("es.port must be defined in the options.")
    }
  }
}
