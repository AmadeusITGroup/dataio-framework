package com.amadeus.dataio.testutils

import scala.collection.JavaConverters._
import scala.language.implicitConversions

/**
 * <p>Contains helper implicit conversions to make working with functions expecting Java maps/lists easier.</p>
 * <p>These conversions are meant for tests only, for instance to create typesafe Config objects.</p>
 * <p> e.g.:
 * <pre>
 * import com.amadeus.dataio.test.JavaImplicitConverters._
 * import com.typesafe.config.ConfigFactory
 *
 * val config = ConfigFactory.parseMap(
 *   Map(
 *     "MyField" -> Seq("val1", "val2", "val3"),
 *     "MyOtherField" -> 5
 *   )
 * )
 * </pre>
 * </p>
 */
object JavaImplicitConverters {
  import scala.language.implicitConversions

  implicit def scalaMap2Java(m: Map[String, _]): java.util.Map[String, _] = {
    toJava(m).asInstanceOf[java.util.Map[String, _]]
  }

  implicit def scalaSeq2Java(s: Seq[_]): java.util.List[_] = {
    toJava(s).asInstanceOf[java.util.List[_]]
  }

  private def toJava(obj: Any): Any = {
    obj match {
      case m: Map[_, _] =>
        m.map { case (key, value) =>
          (key, toJava(value))
        }.asJava
      case i: Iterable[_] =>
        i.map(toJava).asJava
      case _ => obj
    }
  }
}
