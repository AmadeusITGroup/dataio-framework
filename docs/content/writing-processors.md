---
title: Writing your processors
layout: default
nav_order: 4
---
# Writing Your Processors
<details open markdown="block">
  <summary>
    Table of contents
  </summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

--- 

Data processors play a crucial role in the Data I/O framework, allowing you to implement custom data transformation logic within your pipelines. This page will guide you through the process of creating your own data processors using the Data I/O framework.

## Overview
Data processors encapsulate the specific data processing steps required for your ETL pipelines. Each Data I/O application requires a processor that is responsible for manipulating the data according to your business requirements.

## Creating a Processor
In the Data I/O framework, processors play a vital role in encapsulating the data transformation logic within the pipeline. A processor represents a single stage or operation in the pipeline and is responsible for manipulating the data according to your business requirements. There are two types of processor traits implemented in the framework: `Processor` and its child trait `Transformer`.

### Processor Trait

The `Processor` trait is the base trait for creating custom processors. It provides the structure and functionality required to define your data transformation logic. By extending the `Processor` trait, you can create your own custom processors and implement the `run` method with your specific transformation steps.

Here's an example of a custom processor that extends the `Processor` trait:

```scala
import com.amadeus.dataio.{HandlerAccessor, Processor}
import org.apache.spark.sql.SparkSession

case class MyDataProcessor() extends Processor {
  override def run(handlers: HandlerAccessor)(implicit spark: SparkSession): Unit = {
    // Access input data
    val inputData: DataFrame = handlers.input.read("my-input")
    // Perform data transformation
    val transformedData: DataFrame = transformData(inputData)
    // Write transformed data to output
    handlers.output.write("my-output", transformedData)
  }

  private def transformData(inputData: DataFrame): DataFrame = {
    // Custom data transformation logic here
    // Return the transformed DataFrame
    ...
  }
}
```

### Transformer Trait
The `Transformer` trait is a more specific type of processor that simplifies common use cases by implementing the `run` method with a predefined transformation flow. It reads from the first input and writes the result of its `featurize` method to the first output.

Here's an example of a custom transformer that extends the `Transformer` trait:

```scala
import com.amadeus.dataio.{HandlerAccessor, Transformer}
import org.apache.spark.sql.{DataFrame, SparkSession}

case class MyDataTransformer() extends Transformer {
  override def featurize(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    // Custom featurization logic here
    // Return the featurized DataFrame
    ...
  }
}
```

By extending the `Transformer` trait, you only need to implement the `featurize` method, simplifying the code and making it more concise.

--- 

You can also define your own processor traits by extending the `Processor` trait. For more information on that, please visit the [advanced section](advanced/custom-processor-traits.html).

## Custom Configuration

You can access custom configuration options directly within your processor code. The custom configuration options defined under the Processing configuration node can be accessed through the `config` member variable in your processor class.

Here's an example of how to access custom configuration values within your processor:

```scala
case class MyDataProcessor() extends Processor {
  override def run(handlers: HandlerAccessor)(implicit spark: SparkSession): Unit = {
    // Access custom configuration values
    val customValue1 = config.getString("CustomValue1")
    val customValue2 = config.getInt("CustomValue2")

    // Use the custom configuration values in your data transformation logic
    // ...
  }
}
```

In the above example, `CustomValue1` and `CustomValue2` are custom configuration values defined under the Processing configuration node in your configuration file: 

```scala
Processing {
  Type = com.mycompany.MyDataProcessor
  CustomValue1 = "june"
  CustomValue2 = "2023"
}
```

By directly accessing the config member variable in your processor, you can leverage custom configuration options to parameterize and customize the behavior of your processors.