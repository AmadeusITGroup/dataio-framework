package com.amadeus.dataio.test

import scala.language.implicitConversions
import scala.collection.JavaConverters._

/**
 * <p>Contains helper implicit conversions to make working with functions expecting Java maps/lists easier.</p>
 * <p>These conversions are meant for tests only, for instance to create typesafe Config objects.</p>
 * <p> e.g.:
 * <pre>
 * import com.amadeus.dataio.test.JavaImplicitConverters
 * import com.typesafe.config.ConfigFactory
 * import org.scalatest.flatspec.AnyFlatSpec
 *
 * class MyAppSpec extends AnyFlatSpec with JavaImplicitConverters {
 *   "MyApp" should "do something" in {
 *     val config = ConfigFactory.parseMap(
 *       Map(
 *         "MyField" -> Seq("val1", "val2", "val3"),
 *         "MyOtherField" -> 5
 *       )
 *     )
 *   }
 * }
 * </pre>
 * </p>
 */
trait JavaImplicitConverters {
  import scala.language.implicitConversions

  implicit def scalaMap2Java(m: Map[String, _]): java.util.Map[String, _] = {
    toJava(m).asInstanceOf[java.util.Map[String, _]]
  }

  implicit def scalaSeq2Java(s: Seq[_]): java.util.List[_] = {
    val list = new java.util.ArrayList[Any]
    s.foreach(item => list.add(toJava(item)))
    list
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
