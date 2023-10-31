---
title: Custom processor traits
layout: default
parent: Advanced
nav_order: 1
---
# Custom processor traits

Apart from the provided `Processor` and `Transformer` traits, you can define your own custom traits that extend `Processor` to match your specific use cases. This allows you to encapsulate common data transformation patterns or reusable logic into custom traits, making your code even more modular and maintainable.

For example, if your organization regularily needs to join data from two different datasets, you could create a `JoinTransformer` trait, such as:

```scala
import com.amadeus.dataio.{HandlerAccessor, Transformer}
import org.apache.spark.sql.{DataFrame, SparkSession}

trait JoinTransformer() extends Processor {

  override def run(handlers: HandlerAccessor)(implicit spark: SparkSession): Unit = {
    if (handlers.input.getAll.size < 2) throw new Exception("Can not run a JoinTransformer without two inputs configurations.")
    if (handlers.output.getAll.isEmpty) throw new Exception("Can not run a JoinTransformer without an output configuration.")

    val inputData1 = handlers.input.getAll.head
    val inputData2 = handlers.input.getAll(1)

    val transformedData = featurize(inputdata1, inputData2)

    handlers.output.getAll.head.write(transformedData)
  }

  type T

  def featurize(inputData1: DataFrame, inputData2: DataFrame)(implicit spark: SparkSession): Dataset[T]
}
```

Since the trait extends `Processor`, it is compatible with Data I/O and can be used in the configuration file.