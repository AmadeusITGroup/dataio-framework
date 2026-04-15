// ════════════════════════════════════════════════════════════════════════════
//  BUILD SETUP — all version-coupled values come from SparkProfiles
//  See: project/SparkProfiles.scala (the single source of truth)
// ════════════════════════════════════════════════════════════════════════════

val spark = SparkProfiles.active

ThisBuild / organization := "com.amadeus.dataio"
ThisBuild / versionScheme := Some("early-semver")
ThisBuild / scalaVersion := spark.scalaVersion
ThisBuild / javacOptions ++= Seq("-source", spark.javaTarget, "-target", spark.javaTarget)
ThisBuild / scalacOptions += "-deprecation"

ThisBuild / Test / javaOptions ++= Seq(
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
)
// Required for javaOptions to take effect with forked JVM
ThisBuild / Test / fork := true

// ════════════════════════════════════════════════════════════════════════════
//  RELEASE SETUP
// ════════════════════════════════════════════════════════════════════════════

import sbt.Keys.libraryDependencies
import sbtrelease.ReleaseStateTransformations.*

def getReleaseVersion(ver: String, bumpType: String): String = {
  val pattern = """(\d+)\.(\d+)\.(\d+)-(spark[\d.]+)-SNAPSHOT""".r

  ver match {
    case pattern(major, minor, patch, sparkTag) =>
      bumpType match {
        case "MAJOR" => s"${major.toInt + 1}.0.0-$sparkTag"
        case "MINOR" => s"$major.${minor.toInt + 1}.0-$sparkTag"
        case "PATCH" => s"$major.$minor.$patch-$sparkTag"
        case _       => sys.error(s"Invalid RELEASE_TYPE: $bumpType")
      }
    case _ => sys.error(s"Invalid version format: $ver")
  }
}

def getReleaseNextVersion(ver: String): String = {
  val pattern = """(\d+)\.(\d+)\.(\d+)-(spark[\d.]+)""".r

  ver match {
    case pattern(major, minor, patch, sparkTag) =>
      s"$major.$minor.${patch.toInt + 1}-$sparkTag-SNAPSHOT"
    case _ => sys.error(s"Invalid version format: $ver")
  }
}

val bumpType = sys.env.getOrElse("RELEASE_TYPE", "PATCH")
releaseVersion := { getReleaseVersion(_, bumpType) }
releaseNextVersion := { getReleaseNextVersion }

ThisBuild / releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies, // Ensure no SNAPSHOT dependencies exist
  inquireVersions,           // Ask for new version (auto-updated)
  setReleaseVersion,         // Set the new version
  commitReleaseVersion,      // Commit with updated version
  tagRelease,                // Tag in Git
  publishArtifacts,          // Publish JARs
  setNextVersion,            // Set the next development version
  commitNextVersion,         // Commit next version
  pushChanges                // Push everything to Git
)

// ════════════════════════════════════════════════════════════════════════════
//  PUBLISHING SETUP — GitHub Packages
// ════════════════════════════════════════════════════════════════════════════

ThisBuild / credentials += Credentials(
  "GitHub Package Registry",
  "maven.pkg.github.com",
  "",
  sys.env.getOrElse("GITHUB_REGISTRY_TOKEN", "")
)

ThisBuild / publishTo := Some(
  "GitHub Packages" at "https://maven.pkg.github.com/AmadeusITGroup/dataio-framework"
)

// ThisBuild / publishTo := Some(Resolver.file("local-maven", file(Path.userHome.absolutePath + "/.m2/repository")))

ThisBuild / publishMavenStyle := true
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / pomExtra :=
  <url>https://github.com/AmadeusITGroup/dataio-framework</url>
    <licenses>
      <license>
        <name>Apache License 2.0</name>
        <url>https://github.com/AmadeusITGroup/dataio-framework/blob/main/LICENSE</url>
      </license>
    </licenses>

// ════════════════════════════════════════════════════════════════════════════
//  TESTS SETUP
// ════════════════════════════════════════════════════════════════════════════

ThisBuild / Test / parallelExecution := false
ThisBuild / Test / publishArtifact := false

// ════════════════════════════════════════════════════════════════════════════
//  SHARED DEPENDENCIES (driven by SparkProfiles)
// ════════════════════════════════════════════════════════════════════════════

lazy val commonSettings = Seq(
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql"  % spark.sparkVersion,
    "org.apache.spark" %% "spark-core" % spark.sparkVersion,
    "com.typesafe"      % "config"     % spark.typesafeConfigVersion,
    "org.scalatest"    %% "scalatest"  % spark.scalatestVersion % Test,
    "org.scalamock"    %% "scalamock"  % spark.scalamockVersion % Test
  )
)

// ════════════════════════════════════════════════════════════════════════════
//  PROJECTS
// ════════════════════════════════════════════════════════════════════════════

/** Shared traits and functions for testing inside Data I/O sub projects.
  * It should not be published, and only be used in the Data I/O project itself.
  * @see [[test]] For testing applications made with Data I/O.
  */
lazy val testutils = (project in file("testutils"))
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql"  % spark.sparkVersion,
      "org.apache.spark" %% "spark-core" % spark.sparkVersion,
      "com.typesafe"      % "config"     % spark.typesafeConfigVersion,
      "org.scalatest"    %% "scalatest"  % spark.scalatestVersion,
      "org.scalamock"    %% "scalamock"  % spark.scalamockVersion
    ),
    publish / skip := true
  )

lazy val core = (project in file("core"))
  .settings(
    commonSettings,
    name := "dataio-core",
    libraryDependencies ++= Seq(
      "org.slf4j"  % "slf4j-api"  % spark.slf4jApiVersion,
      "commons-io" % "commons-io" % spark.commonsIoVersion
    ),
    Test / baseDirectory := (ThisBuild / baseDirectory).value // <-- fix CWD for forked JVM
  )
  .dependsOn(testutils % Test)

lazy val kafka = (project in file("kafka"))
  .settings(
    commonSettings,
    name := "dataio-kafka",
    libraryDependencies ++= Seq(
      "org.apache.spark"        %% "spark-sql-kafka-0-10"   % spark.sparkVersion,
      "io.github.embeddedkafka" %% "embedded-kafka"         % spark.embeddedKafkaVersion % Test,
      "io.github.embeddedkafka" %% "embedded-kafka-streams" % spark.embeddedKafkaVersion % Test
    ),
    Test / baseDirectory := (ThisBuild / baseDirectory).value // <-- fix CWD for forked JVM
  )
  .dependsOn(core, testutils % Test)

// ── Conditionally-included modules (some connectors don't support all Spark versions) ──

lazy val snowflake = (project in file("snowflake"))
  .settings(
    commonSettings,
    name := "dataio-snowflake",
    libraryDependencies ++= spark.sparkSnowflakeVersion.toSeq.map { v =>
      "net.snowflake" %% "spark-snowflake" % v
    },
    // If the connector is unavailable for this profile, skip publishing an empty JAR
    publish / skip := !spark.supportsSnowflake,
    Test / baseDirectory := (ThisBuild / baseDirectory).value // <-- fix CWD for forked JVM
  )
  .dependsOn(core, testutils % Test)

lazy val elasticsearch = (project in file("elasticsearch"))
  .settings(
    commonSettings,
    name := "dataio-elasticsearch",
    libraryDependencies ++= spark.elasticsearchSparkVersion.toSeq.map { v =>
      "org.elasticsearch" %% "elasticsearch-spark-30" % v exclude
        ("org.scala-lang", "scala-library") exclude
        ("org.scala-lang", "scala-reflect") exclude
        ("org.slf4j", "slf4j-api") exclude
        ("org.apache.spark", s"spark-core_${spark.scalaBinaryVersion}") exclude
        ("org.apache.spark", s"spark-sql_${spark.scalaBinaryVersion}") exclude
        ("org.apache.spark", s"spark-catalyst_${spark.scalaBinaryVersion}") exclude
        ("org.apache.spark", s"spark-streaming_${spark.scalaBinaryVersion}")
    },
    // If the connector is unavailable for this profile, skip publishing an empty JAR
    publish / skip := !spark.supportsElasticsearch,
    Test / baseDirectory := (ThisBuild / baseDirectory).value // <-- fix CWD for forked JVM
  )
  .dependsOn(core, testutils % Test)

lazy val test = (project in file("test"))
  .settings(
    commonSettings,
    name := "dataio-test",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % spark.scalatestVersion,
      "org.scalamock" %% "scalamock" % spark.scalamockVersion
    ),
    Test / baseDirectory := (ThisBuild / baseDirectory).value // <-- fix CWD for forked JVM
  )
  .dependsOn(core, testutils % Test)

// ════════════════════════════════════════════════════════════════════════════
//  ROOT AGGREGATE — dynamically includes only modules available for this profile
// ════════════════════════════════════════════════════════════════════════════

lazy val alwaysModules: Seq[ProjectReference] = Seq(core, test, kafka)

lazy val conditionalModules: Seq[ProjectReference] =
  (if (spark.supportsSnowflake) Seq[ProjectReference](snowflake) else Nil) ++
    (if (spark.supportsElasticsearch) Seq[ProjectReference](elasticsearch) else Nil)

lazy val root = (project in file("."))
  .settings(
    name := "dataio",
    publish / skip := true
  )
  .aggregate(alwaysModules ++ conditionalModules: _*)
