// BUILD SETUP
// All versions come from the selected Spark profile, so that the lines we support stay internally
// consistent. Select one with the SPARK_PROFILE environment variable, and build it with the JDK the
// profile names: selecting it also checks the running JVM. See project/SparkProfile.scala.
val profile = SparkProfile.selected

ThisBuild / organization := "com.amadeus.dataio"
ThisBuild / versionScheme := Some("early-semver")
ThisBuild / scalaVersion := profile.scala

// RELEASE SETUP
import sbt.Keys.libraryDependencies
import sbtrelease.ReleaseStateTransformations.*

// These operate on the plain semver held in version.sbt. The Spark line is not part of it.
def getReleaseVersion(ver: String, bumpType: String): String = {
  val pattern = """(\d+)\.(\d+)\.(\d+)-SNAPSHOT""".r

  ver match {
    case pattern(major, minor, patch) =>
      bumpType match {
        case "MAJOR" => s"${major.toInt + 1}.0.0"
        case "MINOR" => s"$major.${minor.toInt + 1}.0"
        case "PATCH" => s"$major.$minor.$patch"
        case _       => sys.error(s"Invalid RELEASE_TYPE: $bumpType")
      }
    case _ => sys.error(s"Invalid version format: $ver")
  }
}

def getReleaseNextVersion(ver: String): String = {
  val pattern = """(\d+)\.(\d+)\.(\d+)""".r

  ver match {
    case pattern(major, minor, patch) =>
      s"$major.$minor.${patch.toInt + 1}-SNAPSHOT"
    case _ => sys.error(s"Invalid version format: $ver")
  }
}

val bumpType = sys.env.getOrElse("RELEASE_TYPE", "PATCH")
releaseVersion := { getReleaseVersion(_, bumpType) }
releaseNextVersion := { getReleaseNextVersion }

// Bump and tag only: publishing is a separate, per-profile step so that every supported Spark line
// is published from one tag. See .github/workflows/publish.yml.
//
// Scoped to this project, not ThisBuild: sbt-release defines releaseProcess in its projectSettings,
// so a ThisBuild-scoped value is shadowed by the plugin's default and silently ignored.
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies, // Ensure no SNAPSHOT dependencies exist
  inquireVersions,           // Ask for new version (auto-updated)
  setReleaseVersion,         // Set the new version
  commitReleaseVersion,      // Commit with updated version
  tagRelease,                // Tag in Git
  setNextVersion,            // Set the next development version
  commitNextVersion,         // Commit next version
  pushChanges                // Push everything to Git
)

// Global GitHub Packages settings
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
// Additional Maven metadata
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / pomExtra :=
  <url>https://github.com/AmadeusITGroup/dataio-framework</url>
    <licenses>
      <license>
        <name>Apache License 2.0</name>
        <url>https://github.com/AmadeusITGroup/dataio-framework/blob/main/LICENSE</url>
      </license>
    </licenses>

// TESTS SETUP
ThisBuild / Test / parallelExecution := false
ThisBuild / Test / publishArtifact := false

// Later JDKs need Spark to be granted access to JDK internals. Those flags only apply to a forked
// JVM, so fork exactly when the profile asks for them.
ThisBuild / Test / fork := profile.testJavaOptions.nonEmpty
ThisBuild / Test / javaOptions ++= profile.testJavaOptions

// PROJECTS SETUP

/** Appends the Spark line to the plain semver held in version.sbt, for the published modules.
  *
  * Scoped per project rather than on ThisBuild, which would be circular: it is defined in terms of
  * the ThisBuild-scoped value that version.sbt sets. Deliberately not applied to [[root]], which is
  * not published and whose `version` is what the release steps read, bump and tag: they must see
  * plain semver, since one release covers every Spark line.
  */
lazy val versionSettings = Seq(
  version := SparkProfile.publishedVersion((ThisBuild / version).value, profile)
)

lazy val commonSettings = versionSettings ++ Seq(
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql"  % profile.spark,
    "org.apache.spark" %% "spark-core" % profile.spark,
    "com.typesafe"      % "config"     % profile.typesafeConfig,
    "org.scalatest"    %% "scalatest"  % profile.scalatest % Test,
    "org.scalamock"    %% "scalamock"  % profile.scalamock % Test
  )
)

/** Shared traits and functions for testing inside Data I/O sub projects.
  * It should not be published, and only be used in the Data I/O project itself.
  * @see [[test]] For testing applications made with Data I/O.
  */
lazy val testutils = (project in file("testutils"))
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql"  % profile.spark,
      "org.apache.spark" %% "spark-core" % profile.spark,
      "com.typesafe"      % "config"     % profile.typesafeConfig,
      "org.scalatest"    %% "scalatest"  % profile.scalatest,
      "org.scalamock"    %% "scalamock"  % profile.scalamock
    ),
    publish / skip := true
  )

lazy val core = (project in file("core"))
  .settings(
    commonSettings,
    name := "dataio-core",
    libraryDependencies ++= Seq(
      "org.slf4j"  % "slf4j-api"  % profile.slf4jApi,
      "commons-io" % "commons-io" % profile.commonsIo
    )
  )
  .dependsOn(testutils % Test)

lazy val kafka = (project in file("kafka"))
  .settings(
    commonSettings,
    name := "dataio-kafka",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql-kafka-0-10" % profile.spark
    )
  )
  .dependsOn(core, testutils % Test)

lazy val snowflake = (project in file("snowflake"))
  .settings(
    commonSettings,
    name := "dataio-snowflake",
    libraryDependencies ++= Seq(
      "net.snowflake" %% "spark-snowflake" % profile.sparkSnowflake
    )
  )
  .dependsOn(core, testutils % Test)

lazy val elasticsearch = (project in file("elasticsearch"))
  .settings(
    commonSettings,
    name := "dataio-elasticsearch",
    libraryDependencies ++= Seq(
      // The connector's artifact name is per Spark line, not just its version.
      "org.elasticsearch" %% profile.elasticsearchArtifact % profile.elasticsearch
        exclude ("org.scala-lang", "scala-library")
        exclude ("org.scala-lang", "scala-reflect")
        exclude ("org.slf4j", "slf4j-api")
        exclude ("org.apache.spark", "spark-core_" + scalaBinaryVersion.value)
        exclude ("org.apache.spark", "spark-sql_" + scalaBinaryVersion.value)
        exclude ("org.apache.spark", "spark-catalyst_" + scalaBinaryVersion.value)
        exclude ("org.apache.spark", "spark-streaming_" + scalaBinaryVersion.value)
        // The connector pulls spark-yarn from an older Spark line; keep the build on a single Spark version
        exclude ("org.apache.spark", "spark-yarn_" + scalaBinaryVersion.value)
    )
  )
  .dependsOn(core, testutils % Test)

lazy val test = (project in file("test"))
  .settings(
    commonSettings,
    name := "dataio-test",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % profile.scalatest,
      "org.scalamock" %% "scalamock" % profile.scalamock
    )
  )
  .dependsOn(core, testutils % Test)

// Projects configuration
lazy val root = (project in file("."))
  .settings(
    name := "dataio",
    publish / skip := true
  )
  .aggregate(core, test, kafka, snowflake, elasticsearch)
