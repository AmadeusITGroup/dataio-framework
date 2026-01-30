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

`date_filter` always requires a `column` field (where the filter is applied), plus **one** of the following range definitions:

* **Relative range**: `reference` + `offset`
* **Absolute range**: `from` and/or `until` (you can provide both, or only one of them)

`reference`+`offset` and `from`/`until` are mutually exclusive.
{: .warning}

### Common field

* `column` *(required)*: the date column used for filtering.

### Relative range: `reference` + `offset`

* `reference` *(required)*: the anchor date (iso-date format).
* `offset` *(required)*: a duration relative to `reference` (e.g. `-7D`, `-1M`, `+3D`), defining the other bound.

The resulting interval is the range between `reference` and `reference + offset` (order doesn’t matter: the earliest becomes the lower bound, the latest becomes the upper bound).

### Absolute range: `from` / `until`

* `from` *(optional)*: lower bound (inclusive).
* `until` *(optional)*: upper bound (exclusive).

If only `from` is provided, the filter is **open-ended on the upper side**.
If only `until` is provided, the filter is **open-ended on the lower side**.

If both `from` and `until` are provided, `from` must always be strictly lower than `until`.
{: .info}

## Example

### Relative range (reference + offset)

```hocon
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
```

### Absolute range (from + until)

```hocon
input {
  name = "my-input"
  type = "com.amadeus.dataio.pipes.spark.batch.SparkInput"
  format = "delta"
  path = "hdfs://path/to/data"

  date_filter {
    from = "2023-06-24"
    until = "2023-07-01"
    column = "date"
  }
}
```

### Absolute range (only one bound)

Only `from`:

```hocon
date_filter {
  from = "2023-06-24"
  column = "date"
}
```

Only `until`:

```hocon
date_filter {
  until = "2023-07-01"
  column = "date"
}
```
