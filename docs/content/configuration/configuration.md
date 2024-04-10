---
title: Configuration
layout: default
has_children: true
nav_order: 6
---
# Configuration

At the heart of Data I/O is a configuration file that defines the inputs, outputs, and distributors available for your application. The configuration file provides a structured way to specify the components of your ETL pipeline.

It consists of different
- **Processing**: This node defines the transformation step and specifies the fully qualified name of the class responsible for transforming the data.
- **Input**: The input node defines the data sources for your pipeline. It can be a single object or an array of objects, each representing an input configuration. Each specifies the fully qualified name of the class that reads the data, along with any required parameters.
- **Output**: The output node defines the destinations where the transformed data will be written. Similar to the input node, it can be a single object or an array of objects. Each object specifies the fully qualified name of the class responsible for writing the data, along with the necessary parameters.
- **Distribution**: The optional distribution node defines the methods to distribute files after the processing is complete. Here again, it can be a single object or an array of objects. Each distribution object specifies a method for sending files, such as email, SFTP, or other channels. This allows you to automate the distribution of processed data as needed.

In each of these nodes, the Type field is mandatory, representing the fully qualified name of the corresponding class. Additionally, an optional Name field can be specified to provide a unique identifier for the object, making it easily accessible from HandlerAccessor in the code.
{: .important}

Here is an example of configuration file: 

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

Under the hood, Data I/O uses Typesafe config to parse the configuration file, which means you can benefit from every additional feature that it provides. For more information, head to the 
<a href="https://github.com/lightbend/config" target="_blank">dedicated GitHub repository</a>.
{: .info}