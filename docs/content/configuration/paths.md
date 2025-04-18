---
title: Paths
layout: default
parent: Configuration
nav_order: 3
placeholders:
- pattern: "%{from} %{to}"
  fields:
  - name: template
    mandatory: "Yes"
    description: The path template to fill.
    example: template = file_%{from}_%{to}.csv
  - name: date_reference
    mandatory: "Yes"
    description: The date to use when detemplatizing.
    example: date_reference = "2022-01-27"
  - name: date_offset
    mandatory: "Yes"
    description: The offset to use, with respect to Date when detemplatizing.
    example: date_offset = "+5D"
  - name: date_pattern
    mandatory: "Yes"
    description: The output format to use when detemplatizing. It will apply to both %{from} and %{to}, if they are both present.
    example: date_pattern = "yyyyMMdd"
- pattern: "%{date}"
  fields:
  - name: template
    mandatory: "Yes"
    description: The path template to fill.
    example: template = file_%{datetime}.csv
  - name: date
    description: The date to use when detemplatizing.
    example: date = "2022-01-27"
    default: Current date
  - name: date_pattern
    description: The output format to use when detemplatizing.
    example: date_pattern = "yyyyMMdd"
    default: "yyyyMMdd"
- pattern: "%{year} %{month} %{day}"
  fields:
  - name: template
    mandatory: "Yes"
    description: The path template to fill.
    example: template = file_%{year}.csv
  - name: date
    description: The date to use when detemplatizing.
    example: date = "2022-01-27"
    default: Current datetime
- pattern: "%{uuid}"
  fields:
  - name: template
    mandatory: "Yes"
    description: The template to fill with a random, 16-bytes long, UUID.
    example: template = file_%{uuid}.csv

---
# Paths templatization

Data I/O includes a path templatization feature, allowing you to customize input and output paths with ease when applicable. 

This feature facilitates dynamic path generation by replacing placeholders with values from the configuration.  Path templatization is particularly useful for tasks such as managing date ranges and generating unique identifiers.

## Placeholder fields
Some placeholders may only make sense for outputs configuration, even though they technically can be used in inputs (e.g. random uuid).
{: .warning}

{% for placeholder in page.placeholders %}
    
### {{  placeholder.pattern  }}

{% include fields_table.md fields=placeholder.fields %}

{% endfor %}

## Examples

Here's an example of input using the `path` feature without templatization:

```hocon
(...)

output {
  name = "my-output"
  type = "com.amadeus.dataio.pipes.spark.batch.SparkOutput"
  format = "csv"
  path = "hdfs://path/to/data/static.csv"
}

(...)
```

Here's an example of input using the `path` feature with templatization:

````hocon
(...)

output {
  name = "my-output"
  type = "com.amadeus.dataio.pipes.spark.batch.SparkOutput"
  format = "csv"
  path {
    template = "hdfs://path/to/data/file_%{from}_%{to}.csv"
    date_reference = "2022-01-20"
    date_offset = "-1D"
    date_pattern = "yyyyMMdd"
  }
}

(...)
````

Which will result in the following output path: `hdfs://path/to/data/file_20220119_20220120.csv`