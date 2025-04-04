package com.amadeus.dataio.testutils.matchers

import org.apache.spark.sql.Dataset
import org.scalatest.matchers.{MatchResult, Matcher}

private class CountDatasetResultMatcher(expected: Long) extends Matcher[Dataset[_]] {
  def apply(left: Dataset[_]): MatchResult = {
    val actual = left.count()
    MatchResult(
      actual == expected,
      s"Dataset count was $actual, expected $expected",
      s"Dataset count was $expected when it should not have been"
    )
  }
}
