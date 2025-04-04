package com.amadeus.dataio.config

import com.amadeus.dataio.testutils.ConfigCreator
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PipelineConfigTest extends AnyFlatSpec with ConfigCreator with Matchers {
  "PipelineConfig" should "return a PipelineConfig given a typesafe Config object" in {
    val config = createConfig("""
          processing {
            type = "com.Processor"
          }

          input {
            type = "com.Input"
          }

          output {
            type = "com.Output"
          }
        """)

    val result = PipelineConfig(config)
    result.input.nodes.head.typeName shouldBe "com.Input"
    result.processing.nodes.head.typeName shouldBe "com.Processor"
    result.output.nodes.head.typeName shouldBe "com.Output"
  }

  it should "return a PipelineConfig from the absolute path of application.conf" in {
    val result = PipelineConfig(getClass.getResource("/application.conf").getPath)
    result.input.nodes.head.typeName shouldBe "com.DefaultConfigInput"
    result.processing.nodes.head.typeName shouldBe "com.DefaultConfigProcessor"
    result.output.nodes.head.typeName shouldBe "com.DefaultConfigOutput"
  }

  it should "return a PipelineConfig from a relative path to application.conf" in {
    val result = PipelineConfig(getClass.getResource("/application.conf").getPath)
    result.input.nodes.head.typeName shouldBe "com.DefaultConfigInput"
    result.processing.nodes.head.typeName shouldBe "com.DefaultConfigProcessor"
    result.output.nodes.head.typeName shouldBe "com.DefaultConfigOutput"
  }

  it should "return a PipelineConfig from a URL to application.conf" in {
    val result = PipelineConfig(getClass.getResource("/application.conf").getPath)
    result.input.nodes.head.typeName shouldBe "com.DefaultConfigInput"
    result.processing.nodes.head.typeName shouldBe "com.DefaultConfigProcessor"
    result.output.nodes.head.typeName shouldBe "com.DefaultConfigOutput"
  }
}
