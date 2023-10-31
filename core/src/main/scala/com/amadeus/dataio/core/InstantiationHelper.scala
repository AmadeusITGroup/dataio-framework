package com.amadeus.dataio.core

import com.amadeus.dataio.config.ConfigNode

import scala.reflect.runtime.universe.TermName
import scala.reflect.runtime.{universe => ru}
import scala.util.{Failure, Success, Try}

/**
 * Contains a variety of helper functions to facilitate instantiating objects using reflection.
 */
trait InstantiationHelper {

  /**
   * Creates an instance of a given class by using its companion object.
   * @param config The configuration of the object to create.
   * @tparam T The parent class of the class to instantiate.
   * @return A new instance of the subclass of T provided in the config argument.
   * @throws ClassNotFoundException If the class in the config argument can not be found.
   * @throws Exception If the companion object of the class is not defined properly, that is to say missing an apply
   *                   method taking a ConfigNode argument and returning a T. Finally, if the companion object of the
   *                   class throws an exception.
   */
  def instantiateWithCompanionObject[T](config: ConfigNode): T = {
    // The Scala reflection API being difficult to debug, exceptions are
    // thrown here in order to guide the implementation of new entities
    // types and debug safely without prior knowledge of the reflection API.

    // Getting the companion object
    val rm        = ru.runtimeMirror(getClass.getClassLoader)
    val className = config.typeName
    val entityObjSymbol = Try {
      rm.staticModule(className)
    } getOrElse {
      throw new ClassNotFoundException(className)
    }

    val dcObj = Try {
      rm.reflectModule(entityObjSymbol).instance
    } getOrElse {
      throw new ClassNotFoundException(
        s"Cannot find the companion object of $className. This usually happens if a class without a companion object was provided as Type in the configuration."
      )
    }

    // Getting the apply method
    val entityApplyMethodSymbol = Try {
      entityObjSymbol.typeSignature.decl(TermName("apply")).asTerm.alternatives.head.asMethod
    } getOrElse {
      throw new Exception(s"Cannot find the apply method to instantiate $className.")
    }
    val entityApply = rm.reflect(dcObj).reflectMethod(entityApplyMethodSymbol)

    // Instantiating the entity
    Try {
      entityApply(config.config)
    } match {
      case Success(entity) => entity.asInstanceOf[T]
      case Failure(ex) =>
        ex.getClass.getSimpleName match {
          // If the apply method for the entity throws an exception, this will trigger the throw of an InvocationTargetException in the method
          // mirror, which then contains the original Exception as cause.
          case "InvocationTargetException" => throw ex.getCause
          // If the entity's companion object does not exist or its apply method does not have the proper signature, an IllegalArgumentException
          // will be thrown.
          case "IllegalArgumentException" =>
            throw new Exception(s"$className is not a proper Type. This usually happens if the companion object does not have an apply method with a valid signature.")
          // In all other cases, we don't know how to handle the error, so we re-throw the exception as is.
          case _ => throw ex
        }
    }
  }

  /**
   * Creates an instance of a given class by using its empty constructor.
   * @param fullyQualifiedClassName The fully qualified name of the class to instantiate.
   * @tparam T The parent class of the class to instantiate.
   * @return A new instance of the subclass of T provided in the className argument.
   */
  def instantiateWithEmptyConstructor[T](fullyQualifiedClassName: String): T = {
    Class
      .forName(fullyQualifiedClassName)
      .newInstance
      .asInstanceOf[T]
  }
}

/**
 * Contains a variety of helper functions to facilitate instantiating objects using reflection.
 */
object InstantiationHelper extends InstantiationHelper
