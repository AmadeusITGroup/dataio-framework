package com.amadeus.dataio.pipes.snowflake

import com.typesafe.config.Config

object SnowflakeConfigurator {

  /**
   * @param config The typesafe Config object holding the configuration.
   * @return A String of the hostname for the account
   * @throws com.typesafe.config.ConfigException If data is missing in the config argument. See the user documentation
   *         for the expected fields.
   */
  def getSfUrl(implicit config: Config) : String = {
    config.getString("sfUrl")
  }

  /**
   * @param config The typesafe Config object holding the configuration.
   * @return A String of the login name for the snowflake user
   * @throws com.typesafe.config.ConfigException If data is missing in the config argument. See the user documentation
   *                                             for the expected fields.
   */
  def getSfUser(implicit config: Config): String = {
    config.getString("sfUser")
  }

  /**
   * @param config The typesafe Config object holding the configuration.
   * @return A String of the database to use after connecting
   * @throws com.typesafe.config.ConfigException If data is missing in the config argument. See the user documentation
   *                                             for the expected fields.
   */
  def getSfDatabase(implicit config : Config) : String = {
    config.getString("sfDatabase")
  }

  /**
   * @param config The typesafe Config object holding the configuration.
   * @return A String of the schema to use for the session after connecting.
   * @throws com.typesafe.config.ConfigException If data is missing in the config argument. See the user documentation
   *                                             for the expected fields.
   */
  def getSfSchema(implicit config : Config) : String = {
    config.getString("sfSchema")
  }

}
