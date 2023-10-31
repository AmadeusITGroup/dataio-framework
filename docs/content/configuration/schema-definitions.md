---
title: Schema definitions
layout: default
parent: Configuration
nav_order: 2
---
# Schema Definitions

In some cases (e.g. streaming, reading JSON or CSV inputs), defining a schema is a good practice as it will improve the performances by skipping the schema inference step.

The Data I/O gives the possibility to specify a schema when configuring an input. To do so you have to:

* Create a case class which represents the schema,
* Specify the Schema parameter for the Input in the configuration file,
* Register your case class in the `SchemaRegistry` in the main function of your program.

## Creating a schema

Creating a schema is done by creating a case class which will define the properties of your data. You can use every types supported by Spark SQL.  For more information about supproted types, see the list on <a href="https://spark.apache.org/docs/3.1.2/sql-ref-datatypes.html" target="_blank">Spark SQL official website</a>.

For example you can define a simple schema like this:

```scala
package myproject.models
 
import ...
 
case class Query(queryName: String, queryFilters: Seq[String], date: Timestamp)
```

You can also define a more complex schema like this:

```scala
package myproject.models
 
import ...

case class User(userId: String, userOrganization: String)
case class QueryFilter(filterId: Long, filterField: String, filterValue: String)
case class ComplexQuery(queryName: String, queryFilters: Seq[QueryFilter], date: Timestamp, user: User)
```

## Selecting the Schema

To select a schema, you need to add the parameter Schema and specify the fully-qualified name of the case class to the corresponding input node in your configuration file:

```scala
Input {
    ...
    Schema  = myproject.models.ComplexQuery
}
```

A same schema can be used by several inputs, but only one schema can be defined for a given input.
{: .info}

## Registering the schema

The schemas must be registered via the `SchemaRegistry`, which centralizes all available schemas for the Data I/O inputs. To do so,  call the `registerSchema` method within the main function of your application, as shown below:

```scala
package myproject  
 
import com.amadeus.dataio.{Pipeline, SchemaRegistry}
import myproject.models.{ComplexQuery, Query}
 
object MyApplicationPipeline extends App {
 
    val spark: SparkSession = (...)

    SchemaRegistry.registerSchema[Query]()
    SchemaRegistry.registerSchema[ComplexQuery]()
 
    val pipeline = Pipeline("/path/to/application.conf")
 
    pipeline.run(spark)
}
```

As you can see, registering several schemas is as simple as calling `registerSchema` for each schema you want to be registered.

All the schemas defined in the configuration file must be registered before the call to the run function of your pipeline. If a schema is not found in the registry then an IllegalArgumentException is thrown at runtime.
{: .warning}

