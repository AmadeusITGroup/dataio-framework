# Data I/O

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Spark 3.4](https://img.shields.io/badge/Spark-3.4.4-blue)](https://spark.apache.org/releases/spark-release-3-4-4.html)
[![Spark 3.5](https://img.shields.io/badge/Spark-3.5.3-blue)](https://spark.apache.org/releases/spark-release-3-5-3.html)
[![Scala](https://img.shields.io/badge/Scala-2.12.15-red)](https://www.scala-lang.org/)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)][contributing]

Data I/O is an open source project that provides a flexible and scalable framework for data input and output operations in Spark applications. It offers a set of powerful tools and abstractions to simplify and streamline data processing pipelines.

## Features

- Easy-to-use API for defining data processors and transformations
- Seamless integration with popular data storage systems and formats
- Support for batch and streaming data processing
- Extensible architecture for custom data processors and pipelines
- Scalable and fault-tolerant processing using Apache Spark
- **Multi-Spark version support** — builds and publishes for Spark 3.3, 3.4, and 3.5 from a single codebase

## Supported Spark Versions

| Profile    | Spark | Scala | Java | Snowflake | Elasticsearch |
|------------|-------|-------|------|-----------|---------------|
| `spark34`  | 3.4.4 | 2.12  | 11   | ✅         | ✅             |
| `spark35`  | 3.5.3 | 2.12  | 11   | ✅         | ✅             |
| `spark40`  | 4.0.2 | 2.13  | 17   | ❌         | ❌             |

> **Note:** Spark 4.0 support is experimental. Snowflake and Elasticsearch connectors
> do not yet have Spark 4.0–compatible releases.

## Building Locally

Select a Spark profile via the `SPARK_PROFILE` environment variable (defaults to `spark35`):

```bash
# Build for Spark 3.4
SPARK_PROFILE=spark34 sbt compile

# Run tests for Spark 3.4
SPARK_PROFILE=spark34 sbt test

# Package for Spark 3.5 (default)
sbt package
```

All version-coupled dependencies (Spark, Scala, Java target, connectors) are defined in
[`project/SparkProfiles.scala`](project/SparkProfiles.scala) — the single source of truth.

## Getting Started
To get started with Data I/O, please refer to the [documentation][gettingstarted] for installation instructions, usage examples, and API references.

## Documentation

Comprehensive documentation for Data I/O can be found at the [Data I/O documentation website][documentation]. The documentation provides detailed information on installation, usage, and configuration of the framework. It also includes examples and guides to help you get started with building data processing pipelines using Data I/O.

## Issues and Support
If you encounter any issues or require support, please create a new issue on the [GitHub repository][issues].

## Contribution
Contributions to Data I/O are welcome! To contribute, please follow the guidelines outlined in [our contribution guide][contributing].

## License
This project is licensed under the Apache License 2.0 license. See the [LICENSE][license] file for more information.

[gettingstarted]: https://amadeusitgroup.github.io/dataio-framework/getting-started.html
[documentation]: https://amadeusitgroup.github.io/dataio-framework/
[contributing]: CONTRIBUTING.md
[codeofconduct]: CODE_OF_CONDUCT.md
[license]: LICENSE
[repository]: https://github.com/AmadeusITGroup/dataio-framework
[issues]: https://github.com/AmadeusITGroup/dataio-framework/issues
