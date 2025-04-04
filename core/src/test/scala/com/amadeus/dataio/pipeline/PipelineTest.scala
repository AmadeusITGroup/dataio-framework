package com.amadeus.dataio.pipeline

import com.amadeus.dataio.config.PipelineConfig
import com.amadeus.dataio.core.SchemaRegistry
import com.amadeus.dataio.testutils.SparkSpec

class PipelineTest extends SparkSpec {
  "My test batch Pipeline" should "return selected columns From,Date" in sparkTest { spark =>
    val config = PipelineConfig(getClass.getResource("/pipeline/Pipeline/SparkBatchPipelineTest.conf").getPath)
    Pipeline(config).run(spark)

    val outputPath = config.output.nodes.head.config.getString("path")
    val resultsDf  = spark.read.format("parquet").load(outputPath)

    import spark.implicits._
    val expectedDf = Seq(("Paris", "2021-08-19"), ("Turin", "2022-03-02")).toDF()

    resultsDf should containTheSameRowsAs(expectedDf)
  }

  "My test batch Pipeline with schema" should "return selected columns From,Date" in sparkTest { spark =>
    SchemaRegistry.registerSchema[TestTrip]()

    val config = PipelineConfig(getClass.getResource("/pipeline/Pipeline/SparkBatchPipelineSchemaTest.conf").getPath)
    Pipeline(config).run(spark)

    val outputPath = config.output.nodes.head.config.getString("path")
    val resultsDf  = spark.read.format("parquet").load(outputPath)

    import spark.implicits._
    val expectedDf = Seq(("Paris", "2021-08-19"), ("Turin", "2022-03-02")).toDF()

    resultsDf should containTheSameRowsAs(expectedDf)
  }
}
