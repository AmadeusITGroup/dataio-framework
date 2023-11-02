ThisBuild / scalaVersion  := "2.12.12"
ThisBuild / organization  := "com.amadeus.dataio"
ThisBuild / versionScheme := Some("semver-spec")

Test / parallelExecution := false // TODO check pipeline unit tests before setting to true

val sparkVersion = "3.3.2"

lazy val root = (project in file(".")).settings(
  name                       := "dataio-framework",
  version                    := "1.0.0",
  coverageEnabled            := false, // TODO: fix error on Windows + dependency error and set to true
  coverageFailOnMinimum      := true,
  coverageMinimumStmtTotal   := 70,
  coverageMinimumBranchTotal := 90,
  libraryDependencies ++= Seq(
    // Distribution
    "javax.mail" % "mail" % "1.4.7",

    // Inpout / Output
    "com.crealytics" %% "spark-excel" % "3.3.2_0.19.0",

    // Core
    "org.apache.logging.log4j" %% "log4j-api-scala" % "12.0",
    "org.apache.logging.log4j" % "log4j-api" % "2.19.0",
    "com.typesafe" % "config" % "1.4.0",
    "commons-io" % "commons-io" % "2.9.0",

    // Spark
    "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.3.2",
    "org.apache.spark" %% "spark-sql" % "3.3.2",
    "org.apache.spark" %% "spark-core" % "3.3.2",

    // Tests
    "org.scalatest" %% "scalatest" % "3.2.16" % Test,
    "org.scalamock" %% "scalamock" % "5.2.0" % Test
  ),

  // Publishing settings
  publishTo := Some("GitHub Packages" at "https://maven.pkg.github.com/AmadeusITGroup/dataio-framework"),
  credentials += Credentials(
    "GitHub Package Registry",
    "maven.pkg.github.com",
    "",
    sys.env.getOrElse("GITHUB_TOKEN", "")
  ),
  publishMavenStyle := true,
  Test / publishArtifact := false,
  pomIncludeRepository := { _ => true },

  // Artifact metadata
  pomExtra :=
    <description>Automated handling of inputs, outputs and files distribution for Apache Spark Applications.</description>
      <url>https://github.com/AmadeusITGroup/dataio-framework</url>
      <licenses>
        <license>
          <name>Apache License 2.0</name>
          <url>https://github.com/AmadeusITGroup/dataio-framework/blob/main/LICENSE</url>
        </license>
      </licenses>
)
