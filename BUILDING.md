# Building Data I/O

This document describes how the multi-Spark-version build and release system works.

## Architecture Overview

Data I/O publishes artifacts for multiple Apache Spark versions from a **single codebase**.
All version-coupled values are defined in one place:

```
project/SparkProfiles.scala   ← Single source of truth for ALL versioned dependencies
build.sbt                     ← References SparkProfiles.active (no hardcoded versions)
version.sbt                   ← Derives the Spark tag dynamically from the active profile
```

### How It Works

1. **`project/SparkProfiles.scala`** defines a `SparkProfile` case class containing:
   - Spark version (e.g., `3.5.3`)
   - Scala version (e.g., `2.12.15`)
   - Java target (e.g., `11`)
   - All connector library versions (Snowflake, Elasticsearch, Embedded Kafka, etc.)
   - Feature flags (`supportsSnowflake`, `supportsElasticsearch`)

2. **Profile selection** is driven by the `SPARK_PROFILE` environment variable:
   ```bash
   SPARK_PROFILE=spark34 sbt compile
   ```
   If unset, it defaults to `spark35`.

3. **`build.sbt`** reads `SparkProfiles.active` and uses it for all `scalaVersion`,
   `javacOptions`, and `libraryDependencies` settings. Connector modules (Snowflake,
   Elasticsearch) are conditionally included based on profile feature flags.

4. **`version.sbt`** embeds the Spark version in the artifact version string:
   ```
   1.1.1-spark3.5.3-SNAPSHOT
   ```

## Supported Profiles

| Profile    | Spark | Scala   | Java | Snowflake         | Elasticsearch    |
|------------|-------|---------|------|-------------------|------------------|
| `spark34`  | 3.4.4 | 2.12.15 | 11   | 2.16.0-spark_3.4  | 8.17.4           |
| `spark35`  | 3.5.3 | 2.12.15 | 11   | 3.1.1             | 8.17.4           |
| `spark40`  | 4.0.2 | 2.13.17 | 17   | —                 | —                |

## Local Development

```bash
# Default profile (Spark 3.5)
sbt compile test package

# Specific profile
SPARK_PROFILE=spark35 sbt compile test

# Build all profiles locally (shell loop)
for profile in spark34 spark35 spark40; do
  echo "=== Building $profile ==="
  SPARK_PROFILE=$profile sbt clean compile test
done
```

## CI Pipeline

The GitHub Actions CI workflow (`.github/workflows/ci.yml`) uses a **matrix strategy**
to build, test, and package all supported Spark profiles in parallel:

```yaml
strategy:
  matrix:
    include:
      - spark_profile: spark34
        java_version: "11"
      - spark_profile: spark35
        java_version: "11"
      - spark_profile: spark40
        java_version: "17"
```

Each matrix leg runs with the appropriate Java version and `SPARK_PROFILE` env var.
Artifacts are uploaded with profile-specific names (e.g., `Artefacts-spark34`).

## Release Process

The publish workflow (`.github/workflows/publish.yml`) uses a **two-phase approach**:

### Phase 1: Release Tag (runs once)
1. Runs `sbt 'release with-defaults'` with the default Spark profile (`spark35`)
2. This creates the git tag, commits version bumps, and publishes the Spark 3.5 artifacts
3. The release tag is captured as an output for Phase 2

### Phase 2: Publish Other Profiles (matrix, runs in parallel)
1. Checks out the release tag from Phase 1
2. Regenerates `version.sbt` with the correct Spark-tagged version for the profile
   (needed because sbt-release hardcodes the version string at the tag commit)
3. For each additional profile (`spark34`, `spark40`, ...), runs `sbt publish`
4. Each profile publishes artifacts with its own Spark-tagged version string

### Artifact Naming

Published artifacts follow this convention:
```
com.amadeus.dataio:dataio-core_2.12:1.2.0-spark4.0.2
com.amadeus.dataio:dataio-core_2.12:1.2.0-spark3.5.3
com.amadeus.dataio:dataio-core_2.12:1.2.0-spark3.4.4
```

## Adding a New Spark Version

To add support for a new Spark version:

1. **Edit `project/SparkProfiles.scala`** — add a new entry to the `profiles` map:
   ```scala
   "spark41" -> SparkProfile(
     sparkVersion              = "4.1.1",
     scalaVersion              = "2.13.18",
     javaTarget                = "17",
     sparkSnowflakeVersion     = Some("x.y.z"),
     elasticsearchSparkVersion = Some("8.x.y"),
     embeddedKafkaVersion      = "3.6.0"
   )
   ```

2. **Update CI matrix** — add the profile to `.github/workflows/ci.yml`:
   ```yaml
   - spark_profile: spark41
     java_version: "17"
     label: "Spark 4.0 / Java 17"
   ```

3. **Update Publish matrix** — add to `.github/workflows/publish.yml`

4. **Update README.md** — add badge and compatibility table entry

That's it. No changes to `build.sbt` or any source code are needed.

## Removing a Spark Version

1. Remove the entry from `project/SparkProfiles.scala`
2. Remove from CI and Publish workflow matrices
3. Update README

## Connector Availability

Some connectors may not support all Spark versions (e.g., Spark 4.0). The `SparkProfile`
uses `Option[String]` for connector versions:
- `Some("x.y.z")` → connector is included in the build and published
- `None` → connector module is excluded from aggregation and `publish/skip := true`

This is handled automatically — no manual module toggling needed.


