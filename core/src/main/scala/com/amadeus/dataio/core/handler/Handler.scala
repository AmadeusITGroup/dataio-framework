package com.amadeus.dataio.core.handler

import com.amadeus.dataio.core.ConfigurableEntity
import com.typesafe.config.Config

/**
 * Handles
 *
 * @tparam T The type of entities to handle.
 */
abstract class Handler[T <: ConfigurableEntity] {
  protected var entities: Seq[(String, T)] = Seq()

  /**
   * Returns the all of the entities as a sequence.
   * @return A sequence of the configured entities.
   */
  def getAll: Seq[T] = entities.map(_._2)

  /**
   * Retrieves an entity of a specific type.
   * @param name The name of the entity to retrieve.
   * @tparam S The type of the entity to retrieve.
   * @return The entity, casted as an instance of T.
   * @throws Exception if the entity does not exist.
   */
  def getOne[S <: T](name: String): S = {
    entities.toMap
      .getOrElse(
        name,
        throw new Exception(s"Could not find $name.")
      )
      .asInstanceOf[S]
  }

  /**
   * Retrieves the configuration of a handled entity based on its name.
   * @param name The name of the entity.
   * @return The configuration object of the entity.
   * @throws Exception if the entity does not exist.
   */
  def getConfig(name: String): Config = {
    entities.toMap
      .getOrElse(
        name,
        throw new Exception(s"Could not find $name.")
      )
      .config
  }

  /**
   * @return The sequence of configuration objets of all handled entities.
   */
  def getConfig: Seq[Config] = {
    entities.map(e => e._2.config)
  }

  /**
   * Adds an entity and its name to the list of entities.
   * @param name The name of the entity.
   * @param entity The entity to add.
   * @throws Exception if the name is empty or already exists.
   */
  def add(name: String, entity: T): Unit = {
    if (name.isEmpty) throw new Exception("Can not add an entity without a name.")
    if (entities.toMap.contains(name)) throw new Exception(s"Can not add an entity named $name as one already exists.")

    entities = entities :+ (name, entity)
  }
}
