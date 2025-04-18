---
title: Date filters
layout: default
parent: Configuration
nav_order: 4
fields:
  - name: reference
      mandatory: "Yes"
      description: A date in the yyyy-MM-dd format.
    example: reference = "2023-07-01"
  - name: offset
      mandatory: "Yes"
      description: A time delta, expressed as <+/-><N, a number><time unit, e.g. D(ays), H(ours)...>
    example: offset = "-7D"
  - name: column
      mandatory: "Yes"
      description: The name of the column that contains the date to filter.
    example: column = "arrival_date"
---
# Date Filters

It might be necessary for your applications to filter input datasets by a specific a date range. This is made possible
by Data I/O directly in the configuration file, via the `date_filter` input field.

`date_filter`'s availability is decided at pipe level. Please refer to their specific documentation to know whether it
is available.
{: .warning}

## Fields

`date_filter` requires a `reference` and an `offset`, in order to define a date range, as well as a `column` field in
order to specify where to apply the filter.

{% include fields_table.md fields=page.fields %}

If the upper limit of the date range has a time past midnight, it will include the day (e.g. if the upper limit is 2022-09-28, 03h00, the 28th of September will be included in the range). The lower limit of the date range is always included. 
{: .info}

## Example

Here's an example of input using the `date_filter` feature:

```hocon
(...)

input {
  name = "my-input"
  type = "com.amadeus.dataio.pipes.spark.batch.SparkInput"
  format = "delta"
  path = "hdfs://path/to/data"
  date_filter {
    reference = "2023-07-01"
    offset = "-7D"
    column = "date"
    }
}

(...)
```
