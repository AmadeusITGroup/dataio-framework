ThisBuild / scalaVersion := "2.12.12"
ThisBuild / organization := "com.amadeus.dataio"
ThisBuild / versionScheme := Some("strict")

val sparkVersion = settingKey[String]("The version of Spark used for building.")
ThisBuild / sparkVersion := IO.read(baseDirectory.value / "project" / "spark-version.conf").trim

val projectVersion = "1.0.0"

Test / parallelExecution := false // TODO check pipeline unit tests before setting to true

ThisBuild / dependencyOverrides += "org.scala-lang.modules" %% "scala-xml" % "1.2.0" // TODO find a better way to fix dependency compatibility issue between spark-core and scoverage

lazy val root = (project in file(".")).settings(
  name := "dataio-framework",
  version := s"$projectVersion-spark_${toMajorMinorVersion(sparkVersion.value)}",
  coverageEnabled := true,
  coverageFailOnMinimum := true,
  coverageMinimumStmtTotal := 70,
  coverageMinimumBranchTotal := 70,
  libraryDependencies ++= Seq(
    // Distribution
    "javax.mail" % "mail" % "1.4.7",
    // Inpout / Output
    "com.crealytics"    %% "spark-excel"            % s"${sparkVersion.value}_0.19.0",
    "org.elasticsearch" %% "elasticsearch-spark-30" % "8.4.3"
      exclude ("org.scala-lang", "scala-library")
      exclude ("org.scala-lang", "scala-reflect")
      exclude ("org.slf4j", "slf4j-api")
      exclude ("org.apache.spark", "spark-core_" + scalaVersion.value.substring(0, 4))
      exclude ("org.apache.spark", "spark-sql_" + scalaVersion.value.substring(0, 4))
      exclude ("org.apache.spark", "spark-catalyst_" + scalaVersion.value.substring(0, 4))
      exclude ("org.apache.spark", "spark-streaming_" + scalaVersion.value.substring(0, 4)),
    // Core
    "org.apache.logging.log4j" %% "log4j-api-scala" % "12.0",
    "org.apache.logging.log4j"  % "log4j-api"       % "2.19.0",
    "com.typesafe"              % "config"          % "1.4.0",
    "commons-io"                % "commons-io"      % "2.9.0",
    // Spark
    "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion.value,
    "org.apache.spark" %% "spark-sql"            % sparkVersion.value,
    "org.apache.spark" %% "spark-core"           % sparkVersion.value,
    // Tests
    "org.scalatest" %% "scalatest" % "3.2.16" % Test,
    "org.scalamock" %% "scalamock" % "5.2.0"  % Test
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

def toMajorMinorVersion(version: String): String = {
  val versionPattern = """(\d+)\.(\d+)(?:\.(\d+))?""".r

  version match {
    case versionPattern(major, minor, _) => s"$major.$minor"
    case _                               => throw new Exception("Version must comply with semantic versioning format.")
  }
}
