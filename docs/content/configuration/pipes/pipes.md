---
title: Pipes
layout: default
parent: Configuration
has_children: true
nav_order: 10
fields: 
    - name: name
      mandatory: "Yes"
      description: The name of the pipe, that can be used to access it from the HandlerAccessor.
      example: name = "my-input"
    - name: type
      mandatory: "Yes"
      description: The fully qualified name of the class to use as pipe (Input or Output).
      example: type = "com.amadeus.dataio.pipes.spark.batch.SparkInput"
---
# Pipes

Pipes in the Data I/O framework are responsible for handling the reading and writing of data within your pipeline. They provide a convenient and configurable way to interact with different data sources and destinations.

# Common Fields

All pipes have access to at least two fields: `name` and `type`.

{% include fields_table.md fields=page.fields %}

## Batch and Streaming Inputs/Outputs

Due to how Spark operates, it's important to note that the type of input and output you use should match when using the Data I/O framework. If you're reading data from a batch source, such as a batch file or database, it's necessary to use a batch output for writing the transformed data. Similarly, if you're working with a streaming input, such as a streaming data source or a Kafka topic, then a streaming output is necessary for writing the processed data.

While it's technically possible with Spark to mix batch and streaming inputs/outputs (for instance, using `foreachBatch`), it can lead to unexpected behavior or issues. Therefore, it's generally recommended to ensure that the type of input and output you use aligns with each other in your pipeline.

In the following pages, you'll find more detailed information about configuring specific types of inputs and outputs, along with examples and best practices.

