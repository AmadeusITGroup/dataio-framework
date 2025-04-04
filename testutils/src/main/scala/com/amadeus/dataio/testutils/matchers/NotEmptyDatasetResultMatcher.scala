package com.amadeus.dataio.testutils.matchers

import org.apache.spark.sql.Dataset
import org.scalatest.matchers.{MatchResult, Matcher}

private class NotEmptyDatasetResultMatcher extends Matcher[Dataset[_]] {
  def apply(left: Dataset[_]): MatchResult = {
    MatchResult(
      !left.isEmpty,
      "Dataset was empty",
      "Dataset was not empty"
    )
  }
}
