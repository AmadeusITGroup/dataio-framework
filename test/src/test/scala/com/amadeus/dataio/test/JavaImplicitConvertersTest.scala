package com.amadeus.dataio.test

import org.scalatest.flatspec.AnyFlatSpec

class JavaImplicitConvertersTest extends AnyFlatSpec with JavaImplicitConverters {

  "JavaImplicitConverters" should "convert Scala map to Java map" in {
    val scalaMap = Map(
      "key1" -> "value1",
      "key2" -> "value2"
    )

    val javaMap: java.util.Map[String, _] = scalaMap2Java(scalaMap)

    assert(javaMap.containsKey("key1"))
    assert(javaMap.containsKey("key2"))
    assert(javaMap.get("key1") == "value1")
    assert(javaMap.get("key2") == "value2")
  }

  it should "convert Scala sequence to Java list" in {
    val scalaSeq = Seq("value1", "value2", "value3")

    val javaList: java.util.List[_] = scalaSeq2Java(scalaSeq)

    assert(javaList.contains("value1"))
    assert(javaList.contains("value2"))
    assert(javaList.contains("value3"))
  }

  it should "convert nested Scala map to nested Java map" in {
    val scalaNestedMap = Map(
      "nestedMap" -> Map(
        "key1" -> "value1",
        "key2" -> "value2"
      )
    )

    val javaNestedMap: java.util.Map[String, _] = scalaMap2Java(scalaNestedMap("nestedMap").asInstanceOf[Map[String, _]])

    assert(javaNestedMap.containsKey("key1"))
    assert(javaNestedMap.containsKey("key2"))
    assert(javaNestedMap.get("key1") == "value1")
    assert(javaNestedMap.get("key2") == "value2")
  }

  it should "convert nested Scala sequence to nested Java list" in {
    val scalaNestedSeq = Seq(Seq("value1", "value2"), Seq("value3", "value4"))

    val javaNestedList: java.util.List[_] = scalaSeq2Java(scalaNestedSeq.flatMap(identity))

    assert(javaNestedList.contains("value1"))
    assert(javaNestedList.contains("value2"))
    assert(javaNestedList.contains("value3"))
    assert(javaNestedList.contains("value4"))
  }

  it should "handle empty input gracefully" in {
    val emptyMap = Map.empty[String, String]
    val emptySeq = Seq.empty[String]

    val javaEmptyMap: java.util.Map[String, _] = scalaMap2Java(emptyMap)
    val javaEmptyList: java.util.List[_]       = scalaSeq2Java(emptySeq)

    assert(javaEmptyMap.isEmpty)
    assert(javaEmptyList.isEmpty)
  }
}
