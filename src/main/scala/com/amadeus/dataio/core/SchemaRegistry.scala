package com.amadeus.dataio.core

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.StructType

import scala.reflect.runtime.universe._

/**
 * Schema registry used to retrieve schema when reading a dataframe.
 */
object SchemaRegistry extends Logging {

  private var register: Map[String, StructType] = Map.empty

  /**
   * Create an entry for the register based on the case class.
   *
   * @tparam T the case class to be used.
   * @return the entry for the register with the corresponding StructType.
   */
  private def getEntry[T <: Product: TypeTag]: (String, StructType) = {
    (typeOf[T].typeSymbol.fullName, Encoders.product[T].schema)
  }

  /**
   * Add a specific case class' schema to the SchemaRegistry.
   * That schema can then be used to access a specific schema for streaming
   * or batch operations
   *
   * @tparam T the case class type that we want to register in our schema registry.
   * @example `SchemaRegistry.addSchemaType[AutomatedCar]`.
   */
  def registerSchema[T <: Product: TypeTag](): Unit = {
    val entry = getEntry[T]
    logger.info(s"Registering schema: ${entry._1}")
    register += entry
  }

  /**
   * Log the register content.
   */
  def checkRegister(): Unit = {
    register.foreach { case (schemaName, schema) => logger.info(s"$schemaName: $schema") }
  }

  /**
   * Retrieve a StructType schema from a Fully Qualified Name as string.
   *
   * @param fullyQualifiedName Fully Qualified Name of the needed struct type.
   * @return the `StructType`.
   * @throws IllegalArgumentException if the schema if not present.
   */
  @throws(classOf[IllegalArgumentException])
  def getSchema(fullyQualifiedName: String): StructType = {
    register.get(fullyQualifiedName) match {
      case Some(registeredSchema) => registeredSchema
      case None                   => throw new IllegalArgumentException(s"Schema $fullyQualifiedName not found in registry")
    }
  }

}
