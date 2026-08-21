/** A complete, self-consistent set of versions for one supported Spark line.
  *
  * Every field is coupled to the others: a Spark line implies a Scala version, which implies a
  * JDK, which implies which connector and test-library builds exist. None of them can be derived
  * from the others, so they are all stated explicitly and only ever changed as a set.
  *
  * @param id                     Identifies the line in published versions, as `-spark<id>`. Must be the full
  *                               Spark version: lines can differ by patch alone, so a minor-only id would collide.
  * @param java                   The JDK this line must be built with, as reported by `java.specification.version`
  *                               (normalized, i.e. "8" rather than "1.8"). Enforced by a guard in build.sbt.
  * @param testJavaOptions        JVM flags needed to run Spark in-process under this line's JDK. Empty for JDKs
  *                               that need none. When non-empty, tests must be forked for them to take effect.
  * @param elasticsearchArtifact  The connector's artifact name varies per Spark line, not just its version.
  */
case class SparkProfile(
    id: String,
    spark: String,
    scala: String,
    java: String,
    typesafeConfig: String,
    slf4jApi: String,
    commonsIo: String,
    scalatest: String,
    scalamock: String,
    sparkSnowflake: String,
    elasticsearchArtifact: String,
    elasticsearch: String,
    testJavaOptions: Seq[String] = Seq.empty
)

object SparkProfile {

  /** The Spark 3.5.0 line, on Scala 2.12 and Java 8. */
  val spark350: SparkProfile = SparkProfile(
    id                    = "3.5.0",
    spark                 = "3.5.0",
    scala                 = "2.12.15",
    java                  = "8",
    typesafeConfig        = "1.4.3",
    slf4jApi              = "2.0.7",
    commonsIo             = "2.13.0",
    scalatest             = "3.2.15",
    scalamock             = "5.2.0",
    sparkSnowflake        = "3.1.1",
    elasticsearchArtifact = "elasticsearch-spark-30",
    elasticsearch         = "9.0.0"
  )

  val all: Seq[SparkProfile] = Seq(spark350)

  val default: SparkProfile = spark350

  /** Selects the profile to build, from the SPARK_PROFILE environment variable, and checks that the
    * running JVM matches it.
    */
  def selected: SparkProfile = {
    val id = sys.env.getOrElse("SPARK_PROFILE", default.id)

    val profile = all.find(_.id == id).getOrElse {
      sys.error(s"Unknown SPARK_PROFILE '$id'. Supported values: ${all.map(_.id).mkString(", ")}.")
    }

    checkJavaVersion(profile)

    profile
  }

  /** Building a line with the wrong JDK fails deep inside the Scala compiler with an unreadable
    * error (e.g. Scala 2.12.15 on Java 21 dies parsing java.lang.String), so fail early instead.
    */
  private def checkJavaVersion(profile: SparkProfile): Unit = {
    val running = normalizeJavaVersion(sys.props.getOrElse("java.specification.version", "unknown"))

    if (running != profile.java) {
      sys.error(
        s"Spark ${profile.spark} (Scala ${profile.scala}) must be built with Java ${profile.java}, " +
          s"but this JVM is Java $running. Point JAVA_HOME at a Java ${profile.java} JDK."
      )
    }
  }

  /** Normalizes a `java.specification.version` value to the form used by [[SparkProfile.java]],
    * i.e. "1.8" becomes "8".
    */
  def normalizeJavaVersion(version: String): String = version.stripPrefix("1.")

  /** Inserts the Spark line into a base version, keeping any -SNAPSHOT marker last.
    *
    * `version.sbt` holds plain semver so that a single checkout can publish every profile; the
    * Spark line is appended here rather than stored.
    *
    * e.g. ("1.2.0", "3.5.0") => "1.2.0-spark3.5.0"
    *      ("1.2.0-SNAPSHOT", "3.5.0") => "1.2.0-spark3.5.0-SNAPSHOT"
    */
  def publishedVersion(baseVersion: String, profile: SparkProfile): String = {
    val Snapshot = "-SNAPSHOT"

    if (baseVersion.endsWith(Snapshot)) {
      s"${baseVersion.stripSuffix(Snapshot)}-spark${profile.id}$Snapshot"
    } else {
      s"$baseVersion-spark${profile.id}"
    }
  }
}
