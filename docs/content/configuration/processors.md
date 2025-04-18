---
title: Processors
layout: default
parent: Configuration
nav_order: 1
---
# Configuring Your Processors

The Data I/O framework allows you to configure and define your data processors in the application configuration file. This provides flexibility and ease of customization by separating the processor implementation from the configuration details.

## Configuration Syntax

To configure a processor, you need to define it in the application configuration file using the following syntax:

```hocon
processing {
  type = "gettingstarted.DuplicatesDropper"
  custom_value_1 = "june"
  custom_value_2 = "2023"
}
```

To see how to access these values in your processors code, see the [dedicated page](../writing-processors.html#custom-configuration).
