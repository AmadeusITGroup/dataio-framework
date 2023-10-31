---
title: Getting started
layout: default
nav_order: 2
---
# Getting Started
<details open markdown="block">
  <summary>
    Table of contents
  </summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

--- 

## Installation

Data I/O was built with Spark 3.3.2 and Scala 2.12. Support for prior versions is not guaranteed.
{: .warning}

Published releases are available on GitHub Packages, in the AmadeusITGroup repository.

Using Maven:

```xml
<dependency>
    <groupId>com.amadeus.dataio</groupId>
    <artifactId>dataio-framework</artifactId>
    <version>x.x.x</version>
</dependency>
```

Or, using SBT: 

```scala
libraryDependencies += "com.amadeus.dataio" % "data-io-framework" % "x.x.x"
```

--- 


## Minimal Example

This example presents how to write a rudimentary data pipeline: removing the duplicates from a CSV dataset, and saving the result as Parquet.

To make it work, you only need to write three components:

* A data processor, which contains the transformations to operate on the data,
* A configuration file, which contains information about the processor to use, the inputs, outputs, etc.,
* A data pipeline, which loads the configuration file and runs the data processor with the configuration that you defined.

### The Data Processor

Every transformation made using Data I/O must be written in a data processor, a class that you create by extending the Processor trait or one of its sub-classes.

Data transformations happen in the `run` method, which is used by the data pipeline to start the data processing.

```scala
package gettingstarted
 
import com.amadeus.dataio.{HandlerAccessor, Processor}
import org.apache.spark.sql.SparkSession
 
case class DuplicatesDropper() extends Processor {
  override def run(handlers: HandlerAccessor)(implicit spark: SparkSession): Unit = {
  val myInputData = handlers.input.read("my-input")

  val fixedData = myInputData.dropDuplicates

  handlers.output.write("my-output", fixedData)
  }
}
``` 

### The Configuration File

The configuration file contains the definition of the data processor your application will run, as well as inputs, outputs and distributors. In our case, we only need one input, and one output.

```scala
Processing {
    Type = "gettingstarted.DuplicatesDropper"
}
 
Input {
    Name = "my-input"
    Type = "com.amadeus.dataio.pipes.storage.batch.StorageInput"
    Path = "/path/my-input"
    Format = "csv"
    Options {
        header = "true"
    }
}
 
Output {
    Name = "my-output"
    Type = "com.amadeus.dataio.pipes.storage.batch.StorageOutput"
    Path = "/path/my-output"
    Format = "parquet"
}
```

### The Data Pipeline

Now that we're ready, it's time create our pipeline. To do so, we'll use the configuration file.

```scala
package gettingstarted

import com.amadeus.dataio.Pipeline
import org.apache.spark.sql.SparkSession

object MySparkApplication extends App {
  // val spark: SparkSession = (...)
 
  Pipeline("src/main/resources/application.conf").run(spark)
}
```

