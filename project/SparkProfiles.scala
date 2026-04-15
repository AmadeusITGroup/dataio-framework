/** Spark version profiles — the SINGLE SOURCE OF TRUTH for all version-coupled dependencies.
  *
  * Each profile encapsulates:
  *   - Spark version (e.g., 3.3.4, 3.4.3, 3.5.3, 4.0.0)
  *   - Scala version (2.12 or 2.13, derived from Spark requirements)
  *   - Java target compatibility (8, 11, 17, 21)
  *   - All connector library versions (Kafka, Snowflake, Elasticsearch, etc.)
  *   - Test library versions coupled to the Spark/Scala combination
  *
  * Usage:
  *   Set the SPARK_PROFILE environment variable before invoking SBT:
  *     SPARK_PROFILE=spark34 sbt compile
  *
  *   Defaults to "spark35" if not set.
  */
object SparkProfiles {

  case class SparkProfile(
      // ── Core versions ──────────────────────────────────────────────
      sparkVersion: String,
      scalaVersion: String,
      javaTarget: String,
      // ── Connector versions ─────────────────────────────────────────
      sparkSnowflakeVersion: Option[String],
      elasticsearchSparkVersion: Option[String],
      embeddedKafkaVersion: String,
      // ── Common dependency versions ────────────────────────────────
      typesafeConfigVersion: String = "1.4.3",
      slf4jApiVersion: String = "2.0.7",
      commonsIoVersion: String = "2.13.0",
      scalatestVersion: String = "3.2.15",
      scalamockVersion: String = "5.2.0"
  ) {

    /** Major.Minor string, e.g. "3.5" */
    val sparkBinaryVersion: String = sparkVersion.split("\\.").take(2).mkString(".")

    /** Scala binary version, e.g. "2.12" */
    val scalaBinaryVersion: String = scalaVersion.split("\\.").take(2).mkString(".")

    /** Whether this profile supports Snowflake connector */
    val supportsSnowflake: Boolean = sparkSnowflakeVersion.isDefined

    /** Whether this profile supports Elasticsearch connector */
    val supportsElasticsearch: Boolean = elasticsearchSparkVersion.isDefined
  }

  // ════════════════════════════════════════════════════════════════════
  //  Profile definitions — update versions HERE when upgrading
  // ════════════════════════════════════════════════════════════════════

  val profiles: Map[String, SparkProfile] = Map(
    // ── Spark 3.4.x ──────────────────────────────────────────────────
    "spark34" -> SparkProfile(
      sparkVersion = "3.4.4",
      scalaVersion = "2.12.15",
      javaTarget = "11",
      sparkSnowflakeVersion = Some("2.16.0-spark_3.4"),
      elasticsearchSparkVersion = Some("8.17.4"),
      embeddedKafkaVersion = "3.4.1"
    ),
    // ── Spark 3.5.x (default) ────────────────────────────────────────
    "spark35" -> SparkProfile(
      sparkVersion = "3.5.3",
      scalaVersion = "2.12.15",
      javaTarget = "11",
      sparkSnowflakeVersion = Some("3.1.1"),
      elasticsearchSparkVersion = None,
      embeddedKafkaVersion = "3.5.1"
    ),
    // ── Spark 4.0.x ──────────────────────────────────────────────────
    // NOTE: Spark 4.0 drops Scala 2.12 support. Snowflake & ES
    //       connectors may not yet support Spark 4.0 — disabled here.
    "spark40" -> SparkProfile(
      sparkVersion = "4.0.2",
      scalaVersion = "2.13.17",
      javaTarget = "17",
      sparkSnowflakeVersion = None,
      elasticsearchSparkVersion = None,
      embeddedKafkaVersion = "3.9.1"
    )
  )

  // ════════════════════════════════════════════════════════════════════
  //  Active profile resolution
  // ════════════════════════════════════════════════════════════════════

  val DefaultProfile = "spark35"

  val activeProfileName: String = sys.env.getOrElse("SPARK_PROFILE", DefaultProfile)

  val active: SparkProfile = profiles.getOrElse(
    activeProfileName,
    sys.error(s"Unknown SPARK_PROFILE '$activeProfileName'. Available profiles: ${profiles.keys.mkString(", ")}")
  )

  println(
    s"[SparkProfiles] Active profile: $activeProfileName (Spark ${active.sparkVersion}, Scala ${active.scalaVersion}, Java ${active.javaTarget})"
  )
}
