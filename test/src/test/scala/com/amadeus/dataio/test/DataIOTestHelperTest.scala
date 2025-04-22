package com.amadeus.dataio.test

import com.amadeus.dataio.testutils.SparkSpec

class DataIOTestHelperTest extends SparkSpec with DataIOTestHelper {
  behavior of "assertProcessorResult"
  it should "not have any test failures" in sparkTest { implicit spark =>
    val configPath: String = getClass.getResource("/DataIOTestHelper/assertProcessorResult/1_ok/app.conf").getPath
    assertProcessorResult(configPath)
  }

  it should "fail if output doesn't match" in sparkTest { implicit spark =>
    val configPath: String = getClass.getResource("/DataIOTestHelper/assertProcessorResult/2_bad_output/app.conf").getPath
    val thrownEx = intercept[AssertionError] {
      assertProcessorResult(configPath)
    }
    thrownEx.getMessage should include("Content mismatch")
  }

  it should "fail if config has no processor" in sparkTest { implicit spark =>
    val configPath: String = getClass.getResource("/DataIOTestHelper/assertProcessorResult/3_no_processor/app.conf").getPath
    val thrownEx = intercept[Exception] {
      assertProcessorResult(configPath)
    }
    thrownEx.getMessage should include("No `processing` node found in config")
  }

  behavior of "createProcessor(configPath)"
  it should "create a processor from config" in {
    val configPath: String = getClass.getResource("/DataIOTestHelper/createProcessor/1_processor/app.conf").getPath
    val processor          = createProcessor(configPath)
    noException should be thrownBy processor
    processor shouldBe a[DummyTransformer]
  }

  it should "create a transformer from config" in {
    val configPath: String = getClass.getResource("/DataIOTestHelper/createProcessor/2_transformer/app.conf").getPath
    val processor          = createProcessor(configPath)
    noException should be thrownBy processor
    processor shouldBe a[DummyTransformer]
  }

  it should "fail if config has no processor or transformer" in {
    val configPath: String = getClass.getResource("/DataIOTestHelper/createProcessor/3_no_processor/app.conf").getPath
    val thrownEx = intercept[Exception] {
      createProcessor(configPath)
    }
    thrownEx.getMessage should include("No `processing` node found in config")
  }

  it should "throw an exception for invalid processor type" in {
    val configPath: String = getClass.getResource("/DataIOTestHelper/createProcessor/4_invalid_processor/app.conf").getPath
    val thrownEx = intercept[Exception] {
      createProcessor(configPath)
    }
    thrownEx.getMessage should include("Failed to instantiate processor from config")
  }

  behavior of "createProcessor[T]()"
  it should "instantiate a processor using its empty constructor" in {
    val processor = createProcessor[DummyTransformer]()
    noException should be thrownBy processor
    processor shouldBe a[DummyTransformer]
  }

  it should "fail of the given class is not a Processor" in {
    intercept[Exception] {
      createProcessor[String]()
    }
  }
}
