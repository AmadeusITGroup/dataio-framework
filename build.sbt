// BUILD SETUP
ThisBuild / organization := "com.amadeus.dataio"
ThisBuild / versionScheme := Some("early-semver")
ThisBuild / scalaVersion := "2.12.15"

val scalatestVersion      = "3.2.15"
val scalamockVersion      = "5.2.0"
val sparkVersion          = "3.5.0"
val typesafeConfigVersion = "1.4.3"
val slf4jApiVersion       = "2.0.7"
val commonsIoVersion      = "2.13.0"

// RELEASE SETUP
import sbt.Keys.libraryDependencies
import sbtrelease.ReleaseStateTransformations.*

def getReleaseVersion(ver: String, bumpType: String): String = {
  val pattern = """(\d+)\.(\d+)\.(\d+)-(spark[\d.]+)-SNAPSHOT""".r

  ver match {
    case pattern(major, minor, patch, sparkVersion) =>
      bumpType match {
        case "MAJOR" => s"${major.toInt + 1}.0.0-$sparkVersion"
        case "MINOR" => s"$major.${minor.toInt + 1}.0-$sparkVersion"
        case "PATCH" => s"$major.$minor.$patch-$sparkVersion"
        case _       => sys.error(s"Invalid RELEASE_TYPE: $bumpType")
      }
    case _ => sys.error(s"Invalid version format: $ver")
  }
}

def getReleaseNextVersion(ver: String): String = {
  val pattern = """(\d+)\.(\d+)\.(\d+)-(spark[\d.]+)""".r

  ver match {
    case pattern(major, minor, patch, sparkVersion) =>
      s"$major.$minor.${patch.toInt + 1}-$sparkVersion-SNAPSHOT"
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

// PROJECTS SETUP
lazy val commonSettings = Seq(
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql"  % sparkVersion,
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "com.typesafe"      % "config"     % typesafeConfigVersion,
    "org.scalatest"    %% "scalatest"  % scalatestVersion % Test,
    "org.scalamock"    %% "scalamock"  % scalamockVersion % Test
  )
)

/** Shared traits and functions for testing inside Data I/O sub projects.
  * It should not be published, and only be used in the Data I/O project itself.
  * @see [[test]] For testing applications made with Data I/O.
  */
lazy val testutils = (project in file("testutils"))
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql"  % sparkVersion,
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "com.typesafe"      % "config"     % typesafeConfigVersion,
      "org.scalatest"    %% "scalatest"  % scalatestVersion,
      "org.scalamock"    %% "scalamock"  % scalamockVersion
    ),
    publish / skip := true
  )

lazy val core = (project in file("core"))
  .settings(
    commonSettings,
    name := "dataio-core",
    libraryDependencies ++= Seq(
      "org.slf4j"  % "slf4j-api"  % slf4jApiVersion,
      "commons-io" % "commons-io" % commonsIoVersion
    )
  )
  .dependsOn(testutils)

lazy val kafka = (project in file("kafka"))
  .settings(
    commonSettings,
    name := "dataio-kafka",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
    )
  )
  .dependsOn(core, testutils)

lazy val snowflake = (project in file("snowflake"))
  .settings(
    commonSettings,
    name := "dataio-snowflake",
    libraryDependencies ++= Seq(
      "net.snowflake" %% "spark-snowflake" % f"3.1.1"
    )
  )
  .dependsOn(core, testutils)

lazy val elasticsearch = (project in file("elasticsearch"))
  .settings(
    commonSettings,
    name := "dataio-elasticsearch",
    libraryDependencies ++= Seq(
      "org.elasticsearch" %% "elasticsearch-spark-30" % "8.17.4"
        exclude ("org.scala-lang", "scala-library")
        exclude ("org.scala-lang", "scala-reflect")
        exclude ("org.slf4j", "slf4j-api")
        exclude ("org.apache.spark", "spark-core_" + scalaVersion.value.substring(0, 4))
        exclude ("org.apache.spark", "spark-sql_" + scalaVersion.value.substring(0, 4))
        exclude ("org.apache.spark", "spark-catalyst_" + scalaVersion.value.substring(0, 4))
        exclude ("org.apache.spark", "spark-streaming_" + scalaVersion.value.substring(0, 4))
    )
  )
  .dependsOn(core, testutils)

lazy val test = (project in file("test"))
  .settings(
    commonSettings,
    name := "dataio-test",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % scalatestVersion,
      "org.scalamock" %% "scalamock" % scalamockVersion
    )
  )

// Projects configuration
lazy val root = (project in file("."))
  .settings(
    name := "dataio",
    publish / skip := true
  )
  .aggregate(core)
