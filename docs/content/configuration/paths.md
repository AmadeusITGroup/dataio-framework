---
title: Paths
layout: default
parent: Configuration
nav_order: 3
placeholders:
- pattern: "%{from} %{to}"
  fields:
  - name: Template
    mandatory: "Yes"
    description: The path template to fill.
    example: Template = file_%{from}_%{to}.csv
  - name: Date
    mandatory: "Yes"
    description: The date to use when detemplatizing.
    example: Date = "2022-01-01"
  - name: DateOffset
    mandatory: "Yes"
    description: The offset to use, with respect to Date when detemplatizing.
    example: DateOffset = "+5D"
  - name: DatePattern
    mandatory: "Yes"
    description: The output format to use when detemplatizing. It will apply to both %{from} and %{to}, if they are both present.
    example: DatePattern = "yyyyMMdd"
- pattern: "%{datetime}"
  fields:
  - name: Template
    mandatory: "Yes"
    description: The path template to fill.
    example: Template = file_%{datetime}.csv
  - name: Date
    description: The date to use when detemplatizing.
    example: Date = "2022-01-01"
    default: Current datetime
  - name: DatePattern
    description: The output format to use when detemplatizing.
    example: DatePattern = "yyyyMMdd"
    default: "yyyy-MM-dd'T'HH:mm:ss.SSS"
- pattern: "%{year} %{month} %{day}"
  fields:
  - name: Template
    mandatory: "Yes"
    description: The path template to fill.
    example: Template = file_%{year}.csv
  - name: Date
    description: The date to use when detemplatizing.
    example: Date = "2022-01-01"
    default: Current datetime
- pattern: "%{uuid}"
  fields:
  - name: Template
    mandatory: "Yes"
    description: The template to fill with a random, 16-bytes long, UUID.
    example: Template = file_%{uuid}.csv

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



