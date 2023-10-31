---
title: Date filters
layout: default
parent: Configuration
nav_order: 4
fields: 
    - name: Reference
      mandatory: "Yes"
      description: A date in the yyyy-MM-dd format.
      example: Reference = "2023-07-01"
    - name: Offset
      mandatory: "Yes"
      description: A time delta, expressed as <+/-><N, a number><time unit, e.g. D(ays), H(ours)...> 
      example: Offset = "-7D"
    - name: Column
      mandatory: "Yes"
      description: The name of the column that contains the date to filter.
      example: Column = "arrival_date"
---
# Date Filters

It might be necessary for your applications to filter input datasets by a specific a date range. This is made possible by Data I/O directly in the configuration file, via the DateFilter input field.

DateFilter's availability is decided at pipe level. Please refer to their specific documentation to know whether DateFilter is available.
{: .warning}

## Fields
DateFilter requires a Reference and an Offset, in order to define a date range, and a Column, in order to specify where to apply the filter.

{% include fields_table.md fields=page.fields %}

If the upper limit of the date range has a time past midnight, it will include the day (e.g. if the upper limit is 2022-09-28, 03h00, the 28th of September will be included in the range). The lower limit of the date range is always included. 
{: .info}

## Example
Here's an example of input using the DateFilter:
```scala
(...)

Input {
    Type = "com.amadeus.dataio.pipes.storage.batch.StorageInput"
    Format = "delta"
    Path = "hdfs://path/to/data"
    DateFilter {
        Reference = "2023-07-01"
        Offset = "-7D"
        Column = "date"
    }
}

(...)
```
