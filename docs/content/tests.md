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

Data I/O offers a separate library with utility traits and methods designed to facilitate testing Data I/O applications.

The testing strategy for your pipelines is built around flexibility and determinism. It allows you to verify the
correctness of data processing logic without relying on external storage or infrastructure. This is achieved by using:

- In-memory inputs and outputs (`TestInput`, `TestOutput`) backed by an in-memory test data store (`TestDataStore`)
- Reusable testing logic and assertions (`DataIOTestHelper`)

## Installation

Using Maven:

```xml
<dependency>
    <groupId>com.amadeus.dataio</groupId>
    <artifactId>dataio-test</artifactId>
    <version>x.x.x</version>
</dependency>
```

Published releases are available on GitHub Packages, in the Data I/O repository.
{: .info}

## Features Overview

### TestDataStore

`TestDataStore` is a simple in-memory registry that:

- Saves a DataFrame under a unique path
- Loads it back by that path
- Clears a DataFrame at a given path (or all DataFrames with a given path prefix)

In summary, this avoids the need for file I/O or external data systems to write tests results.

### TestInput, TestOutput

These are special types of Input and Output used during testing to decouple test results from real file systems or
databases:

- `TestOutput` captures the results of a processing stage. It stores the resulting DataFrame into the `TestDataStore`.
- `TestInput` simulates reading data from an external source. It loads a DataFrame from the `TestDataStore` using a
  provided
  path

TestInput requires the TestDataStore to be pre-populated. A possible use case is chaining multiple processors, where the
TestOutput of one becomes the TestInput of the next.
{: .warning}

### DataIOTestHelper

This reusable test trait helps streamline testing of end-to-end processor logic.

Key features are:

- `createProcessor(...)`: Parses the pipeline config and instantiates the processor.
- `createHandlers(...)`: Sets up input/output handlers from the same config.
- `runProcessor(...)`: Executes the processor from a given configuration file.
- `assertProcessorResult(...)`: Runs the pipeline and compares each output with its associated expected dataset using:
  - Schema equality
  - Content equality (using except)
  - Row count match

## Testing Patterns

Data I/O’s testing framework supports different levels of test automation, depending on the desired scope and complexity
of your test scenario. You can start with low-level manual testing and scale up to fully automated validation of
processor classes.

### 🔧 Unit Testing (Manual)

Manual testing allows you to test your processor logic in a unit-test-like fashion. This is ideal for focused validation
of internal methods, especially when working with small, handcrafted datasets.

You can create a processor instance manually via the `createProcessor` methods.

There are two main variants:

- Without config file:

````scala 
val processor = createProcessor[MyProcessor]() 
````

This creates an instance of your processor class using a default empty config. It's useful when the logic you want to
test does not depend on any external configuration fields.

- With a config file:
```scala
val processor = createProcessor("/path/to/test/app.conf")
```

This loads configuration values from the specified file. Useful when your processor or its functions rely on config
fields to operate correctly.

#### Example

````scala
val processor = createProcessor[MyProcessor]()
val inputDF = spark.read.parquet("/some/test/input")
val outputDF = processor.myFilter(inputDF)
assert(outputDF.count() === expectedCount)
````

### ⚙️ Functional Testing (Semi-Automated)

This approach uses runProcessor(...) to execute a real test config that defines input/output pipes, letting you test
processor behavior more holistically. It’s useful for functional or integration-style tests.

You provide:

- A test config file that defines test inputs and outputs.
- An expected dataset to compare against.

The logic for validation (e.g., comparing DataFrames) is still implemented in your test class, so you retain flexibility
over what to check.

#### Example

```hocon
processing {
  type = "com.example.MyProcessor"
}

input {
  type = "com.amadeus.dataio.pipes.spark.batch.SparkInput"
  name = "my-test-input"
  path = "/path/to/input"
  format = "csv"
  options {
    header = "true"
  }
}

output {
  type = "com.amadeus.dataio.test.TestOutput"
  name = "my-test-output"
  path = "/path/to/output"
}
```

```scala
runProcessor("/path/to/test/app.conf")

val resultDf = TestDataStore.load("/path/to/output")
val expectedDF = spark.read.option("header", "true").csv("/path/to/expected")

assert(resultDf.except(expectedDF).isEmpty)
assert(expectedDF.except(resultDf).isEmpty)
```

### 🤖 Functional Testing (Fully-Automated)

This approach builds on the semi-automated one but uses `assertProcessorResult(...)`, which encapsulates test execution
and validation. Your config file defines:

- Input pipes
- Output pipes (`TestOutput` or `SparkOutput`)
- Expected datasets, directly associated with outputs

The framework will:

- Load the inputs
- Run the processor
- Compare outputs to expected datasets on:
  - Schema equality
  - Content equality (using except)
  - Row count match

This is the most automated pattern available, but it does not offer the flexibility of the semi-automated approach.

#### Example

```hocon
processing {
  type = "com.example.MyProcessor"
}

input {
  type = "com.amadeus.dataio.pipes.spark.batch.SparkInput"
  name = "my-test-input"
  path = "/path/to/input"
  format = "csv"
  options {
    header = "true"
  }
}

output {
  type = "com.amadeus.dataio.test.TestOutput"
  name = "my-test-output"
  path = "/path/to/output"
  expected {
    path = "/path/to/expected"
    format = "csv"
    options {
      header = "true"
    }
  }
}
```

```scala
assertProcessorResult("/path/to/test/app.conf")
```

### 🧬 Chaining Processors Tests

This pattern is useful to test complete pipelines made of multiple processors, each defined in its own config file. It’s
particularly useful when validating how processors behave together as a pipeline, without needing to persist
intermediate results to disk.

The typical setup is:

- The first processor reads input data from disk using a standard `SparkInput`.
- It writes its results to memory via `TestOutput`
- The next processor(s) read from memory using `TestInput`, and write again to memory using `TestOutput`.

Each step reuses the same TestDataStore, chaining processors entirely in memory after the initial input.

#### Example

```hocon
processing {
  type = "com.example.MyProcessor1"
}

input {
  name = "my-test-input"
  type = "com.amadeus.dataio.pipes.spark.batch.SparkInput"
  path = "/path/to/input"
  format = "csv"
  options {
    header = "true"
  }
}

output {
  name = "my-test-output"
  type = "com.amadeus.dataio.test.TestOutput"
  path = "/path/to/output/1"
}
```

```hocon
processing {
  type = "com.example.MyProcessor2"
}

input {
  name = "my-test-input"
  type = "com.amadeus.dataio.test.TestInput"
  path = "/path/to/output/1"
}

output {
  name = "my-test-output"
  type = "com.amadeus.dataio.test.TestOutput"
  path = "/path/to/output/2"
}
```

```scala
runProcessor("/path/to/processor1.conf")
runProcessor("/path/to/processor2.conf")

val resultDf = TestDataStore.load("/path/to/output/2")
val expectedDF = spark.read.option("header", "true").csv("/path/to/expected")

assert(resultDf.except(expectedDF).isEmpty)
assert(expectedDF.except(resultDf).isEmpty)
```