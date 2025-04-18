---
title: Configuration
layout: default
has_children: true
nav_order: 6
---
# Configuration

At the heart of Data I/O is a configuration file that defines the inputs and outputs available for your application. The
configuration file provides a structured way to specify the components of your ETL pipeline.

It consists of different root notes:

- **processing**: This node defines the transformation step and specifies the fully qualified name of the class
  responsible for transforming the data.
- **input**: The input node defines the data sources for your pipeline. It can be a single object or an array of
  objects, each representing an input configuration. Each specifies the fully qualified name of the class that reads the
  data, along with any required parameters.
- **output**: The output node defines the destinations where the transformed data will be written. Similar to the input
  node, it can be a single object or an array of objects. Each object specifies the fully qualified name of the class
  responsible for writing the data, along with the necessary parameters.

In each of these nodes, the Type field is mandatory, representing the fully qualified name of the corresponding class.
The optional `name` field must be specified to provide a unique identifier for the object, making it easily accessible
from HandlerAccessor in the code.
{: .important}

Here is an example of configuration file: 

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

Under the hood, Data I/O uses Typesafe Config from Lightbend to parse the configuration file, which means you can
benefit from every additional feature that it provides. For more information, head to the
<a href="https://github.com/lightbend/config" target="_blank">dedicated GitHub repository</a>.
{: .info}