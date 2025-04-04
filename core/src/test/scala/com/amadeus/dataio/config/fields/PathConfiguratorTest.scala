package com.amadeus.dataio.config.fields

import com.amadeus.dataio.core.time.DateRange
import com.amadeus.dataio.testutils.ConfigCreator
import com.typesafe.config.Config
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID

class PathConfiguratorTest extends AnyFlatSpec with ConfigCreator with Matchers {
  behavior of "getPath"
  it should "return None when no path configuration is provided" in {
    implicit val config: Config = createConfig("""{}""")

    getPath shouldBe None
  }

  it should "return a simple path when defined directly" in {
    implicit val config: Config = createConfig(
      """
        {
          path = "/data/simple/path"
        }
        """
    )

    getPath shouldBe Some("/data/simple/path")
  }

  it should "resolve a template with %{from} placeholder" in {
    implicit val config: Config = createConfig(
      """
        {
          path.template = "/data/%{from}/files"
          path.date_reference = "2025-01-01"
          path.date_offset = "+30D"
          path.date_pattern = "yyyy-MM-dd"
        }
        """
    )

    val expected = {
      val dateRange = DateRange("2025-01-01", "+30D")
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
      val fromDate  = dateRange.from.format(formatter)
      s"/data/$fromDate/files"
    }

    getPath shouldBe Some(expected)
  }

  it should "resolve a template with %{to} placeholder" in {
    implicit val config: Config = createConfig(
      """
        {
          path.template = "/data/%{to}/files"
          path.date_reference = "2025-01-01"
          path.date_offset = "+30D"
          path.date_pattern = "yyyy-MM-dd"
        }
        """
    )

    val expected = {
      val dateRange = DateRange("2025-01-01", "+30D")
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
      val toDate    = dateRange.until.format(formatter)
      s"/data/$toDate/files"
    }

    getPath shouldBe Some(expected)
  }

  it should "resolve a template with %{date} placeholder" in {
    implicit val config: Config = createConfig(
      """
        {
          path.template = "/data/%{date}/files"
          path.date = "2025-01-15"
          path.date_pattern = "yyyyMMdd"
        }
        """
    )

    // Expected format from the parseDateAsString method
    val expected = "/data/20250115/files"

    getPath.get should be(expected)
  }

  it should "resolve a template with %{uuid} placeholder" in {
    implicit val config: Config = createConfig(
      """
        {
          path.template = "/data/%{uuid}/files"
        }
        """
    )

    val path = getPath.get
    path should startWith("/data/")
    path should endWith("/files")

    // Extract the UUID part and verify it's a valid UUID
    val uuidPattern       = "/data/([^/]+)/files".r
    val uuidPattern(uuid) = path
    noException should be thrownBy UUID.fromString(uuid)
  }

  it should "resolve a template with %{year}, %{month}, %{day} placeholders" in {
    implicit val config: Config = createConfig(
      """
        {
          path.template = "/data/%{year}/%{month}/%{day}/files"
          path.date = "2025-03-15"
        }
        """
    )

    val expected = "/data/2025/03/15/files"
    getPath shouldBe Some(expected)
  }

  it should "handle multiple placeholders in one template" in {
    implicit val config: Config = createConfig(
      """
        {
          path.template = "/data/%{year}/%{month}/from_%{from}_to_%{to}/%{uuid}"
          path.date = "2025-03-15"
          path.date_reference = "2025-01-01"
          path.date_offset = "+30D"
          path.date_pattern = "yyyyMMdd"
        }
        """
    )

    val path = getPath.get

    // Check year, month portions
    path should startWith("/data/2025/03/from_")

    // Extract the UUID and verify it's valid
    val lastSegment = path.split("/").last
    noException should be thrownBy UUID.fromString(lastSegment)

    // Verify the from/to dates are in the format
    val dateRange = DateRange("2025-01-01", "+30D")
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val fromDate  = dateRange.from.format(formatter)
    val toDate    = dateRange.until.format(formatter)

    path should include(s"from_${fromDate}_to_${toDate}")
  }

  it should "use current date when path.date is not specified" in {
    implicit val config: Config = createConfig(
      """
        {
          path.template = "/data/%{year}/%{month}/%{day}/files"
        }
        """
    )

    // Extract the year, month, day from the generated path
    val path                      = getPath.get
    val pattern                   = "/data/(\\d{4})/(\\d{2})/(\\d{2})/files".r
    val pattern(year, month, day) = path

    // Get the current date for comparison
    val now = LocalDateTime.now()

    // Verify the path uses today's date (allowing for timezone differences)
    year should be(now.getYear.toString)
    month.toInt should be >= (now.getMonthValue - 1)
    month.toInt should be <= (now.getMonthValue + 1)
    day.toInt should be >= (if (now.getDayOfMonth > 2) now.getDayOfMonth - 2 else 1)
    day.toInt should be <= (if (now.getDayOfMonth < 29) now.getDayOfMonth + 2 else 31)
  }

  it should "use the default date format when pattern not specified" in {
    implicit val config: Config = createConfig(
      """
        {
          path.template = "/data/%{date}/files"
          path.date = "2025-01-15"
        }
        """
    )

    val path = getPath.get
    path should startWith("/data/")
    path should endWith("/files")

    path should include("20250115")
  }

  it should "handle negative date offsets" in {
    implicit val config: Config = createConfig(
      """
        {
          path.template = "/data/%{from}_%{to}/files"
          path.date_reference = "2025-01-15"
          path.date_offset = "-7D"
          path.date_pattern = "yyyyMMdd"
        }
        """
    )

    val dateRange = DateRange("2025-01-15", "-7D")
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val fromDate  = dateRange.from.format(formatter)
    val toDate    = dateRange.until.format(formatter)

    val expected = s"/data/${fromDate}_${toDate}/files"
    getPath shouldBe Some(expected)
  }
}
