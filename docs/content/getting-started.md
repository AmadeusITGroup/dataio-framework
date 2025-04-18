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

Using Maven:

```xml
<dependency>
    <groupId>com.amadeus.dataio</groupId>
    <artifactId>dataio-core</artifactId>
    <version>x.x.x</version>
</dependency>
```

Published releases are available on GitHub Packages, in the Data I/O repository.
{: .info}

## Minimal Example

This example presents how to write a rudimentary data pipeline: removing the duplicates from a CSV dataset, and saving the result as Parquet.

To make it work, you only need to write three components:

* A data processor, which contains the transformations to operate on the data,
* A configuration file, which contains information about the processor to use, the inputs, outputs, etc.,
* A Pipeline object, which loads the configuration file and runs the data processor with the configuration that you
  defined.

### The Data Processor

Every transformation made using Data I/O must be written in a data processor, a class that you create by extending the Processor trait or one of its sub-classes.

Data transformations happen in the `run` method, which is used by the Pipeline to start the data processing.

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

The configuration file contains the definition of the data processor your application will run, as well as inputs and
outputs. In our case, we only need one input, and one output.

```hocon
processing {
  type = "gettingstarted.DuplicatesDropper"
}

input {
  name = "my-input"
  type = "com.amadeus.dataio.pipes.spark.batch.SparkInput"
  path = "/path/my-input"
  format = "csv"
  options {
        header = "true"
    }
}

output {
  name = "my-output"
  type = "com.amadeus.dataio.pipes.spark.batch.SparkOutput"
  path = "/path/my-output"
  format = "parquet"
}
```

### The Data Pipeline

Now that we're ready, it's time create our Pipeline and run the Processor. To do so, we'll use the configuration file
and a Spark session.

```scala
package gettingstarted

import com.amadeus.dataio.Pipeline
import org.apache.spark.sql.SparkSession

object MySparkApplication extends App {
  // val spark: SparkSession = (...)
 
  Pipeline("src/main/resources/application.conf").run(spark)
}
```

