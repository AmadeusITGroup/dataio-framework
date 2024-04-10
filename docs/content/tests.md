---
title: Writing tests
layout: default
nav_order: 5
---
# Writing tests
<details open markdown="block">
  <summary>
    Table of contents
  </summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

--- 

Data I/O offers a separate library with utility traits and methods designed to facilitate testing Scala/Spark SQL applications. 

## Installation

Published releases are available on GitHub Packages, in the AmadeusITGroup repository.

Using Maven:

```xml
<dependency>
    <groupId>com.amadeus.dataio</groupId>
    <artifactId>dataio-test</artifactId>
    <version>x.x.x</version>
</dependency>
```

## Overview


### Interacting with the file system
The `FileSystemSpec` trait provides the Hadoop `LocalFileSystem` for tests needing direct access to an instance of `FileSystem`.

Example:

```scala

import com.amadeus.dataio.test._
import org.scalatest.flatspec.AnyFlatSpec

case class MyAppTest extends AnyFlatSpec with FileSystemSpec {
  "MyAppTest" should "do something" in {
    assert(fs.exists("file:///my_file.txt"))
  }
}
```


### Interacting with a SparkSession
The `SparkSpec` trait provides a local Spark session and helper functions for Spark tests:
- `getTestName: String`: Returns the test suite's name.
- `collectData(path: String, format: String, schema: Option[String] = None): Array[String])`: Collects data from the file system.

Note that extending this trait, you will have to override the getTestName: String function.

Example:

```scala

import com.amadeus.dataio.test._
import org.scalatest.flatspec.AnyFlatSpec

case class MyAppTest extends AnyFlatSpec with SparkSpec {
  override def getTestName = "MyAppTest"
  
  "MyAppTest" should "do something" in {
    spark.read.format("csv").load("my_data.csv")
    collectData
  }
}
```


### Interacting with a Streaming context
The `SparkStreamingSpec` trait provides a local Spark session and helper functions for Spark Streaming tests:
- `enableSparkStreamingSchemaInference(): Unit`: Enables Spark streaming schema inference.
- `collectDataStream(dataFrame: DataFrame): Array[String]`: Collects data from a DataFrame read from a stream using an in-memory sink.


### Implicitly converting Scala Maps and Lists in Java equivalents
It it sometimes necessary to build complex map structures while building `Typesafe Config` objects, requiring redundant Scala-to-Java conversions.

To simplify this, you may extend the `JavaImplicitConverters` trait.

Example:

```scala

import com.amadeus.dataio.test._
import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec

case class MyAppTest extends AnyFlatSpec with JavaImplicitConverters {
  "MyAppTest" should "do something" in {
    ConfigFactory.parseMap(
      Map("NodeName" -> Seq(Map("Type" -> "com.Entity"), Map("Type" -> "com.Entity")))
    )
  }
}
```

