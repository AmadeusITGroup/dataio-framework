ThisBuild / versionScheme := Some("strict")

// Build configuration
ThisBuild / scalaVersion := "2.12.12"
ThisBuild / organization := "com.amadeus.dataio"
val sparkVersion = settingKey[String]("The version of Spark used for building.")
ThisBuild / sparkVersion := "3.4.1"

// Common dependencies
ThisBuild / libraryDependencies ++= Seq(
  // Core
  "org.apache.logging.log4j" %% "log4j-api-scala" % "12.0",
  "org.apache.logging.log4j"  % "log4j-api"       % "2.19.0",
  "com.typesafe"              % "config"          % "1.4.0",
  "commons-io"                % "commons-io"      % "2.9.0",
  // Spark
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion.value,
  "org.apache.spark" %% "spark-sql"            % sparkVersion.value,
  "org.apache.spark" %% "spark-core"           % sparkVersion.value,
  "net.snowflake"    %% "spark-snowflake"      % f"2.15.0-spark_3.4"
)

// Tests configuration
ThisBuild / Test / parallelExecution := false
ThisBuild / Test / publishArtifact := false

// Publication configuration
ThisBuild / publishTo := Some("GitHub Packages" at "https://maven.pkg.github.com/AmadeusITGroup/dataio-framework")
ThisBuild / credentials += Credentials(
  "GitHub Package Registry",
  "maven.pkg.github.com",
  "",
  sys.env.getOrElse("GITHUB_REGISTRY_TOKEN", "")
)
ThisBuild / publishMavenStyle := true
ThisBuild / pomIncludeRepository := { _ => true }
ThisBuild / pomExtra :=
  <url>https://github.com/AmadeusITGroup/dataio-framework</url>
    <licenses>
      <license>
        <name>Apache License 2.0</name>
        <url>https://github.com/AmadeusITGroup/dataio-framework/blob/main/LICENSE</url>
      </license>
    </licenses>

// Release configuration
import ReleaseTransformations._

releaseVersionBump := sbtrelease.Version.Bump.Minor
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  publishArtifacts,
  setNextVersion,
  commitNextVersion,
  pushChanges
)

// Projects configuration
lazy val root = (project in file("."))
  .aggregate(core, test)
  .settings(
    publishArtifact := false
  )

lazy val core = (project in file("core"))
  .settings(
    name := "dataio-core",
    libraryDependencies ++= Seq(
      // Distribution
      "javax.mail" % "mail" % "1.4.7",
      // Input / Output
      "com.crealytics"    %% "spark-excel"            % s"${sparkVersion.value}_0.19.0",
      "org.elasticsearch" %% "elasticsearch-spark-30" % "8.4.3"
        exclude ("org.scala-lang", "scala-library")
        exclude ("org.scala-lang", "scala-reflect")
        exclude ("org.slf4j", "slf4j-api")
        exclude ("org.apache.spark", "spark-core_" + scalaVersion.value.substring(0, 4))
        exclude ("org.apache.spark", "spark-sql_" + scalaVersion.value.substring(0, 4))
        exclude ("org.apache.spark", "spark-catalyst_" + scalaVersion.value.substring(0, 4))
        exclude ("org.apache.spark", "spark-streaming_" + scalaVersion.value.substring(0, 4)),
      // Tests
      "org.scalatest" %% "scalatest" % "3.2.16" % Test,
      "org.scalamock" %% "scalamock" % "5.2.0"  % Test
    )
  )

lazy val test = (project in file("test"))
  .settings(
    name := "dataio-test",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.16",
      "org.scalamock" %% "scalamock" % "5.2.0"
    )
  )
