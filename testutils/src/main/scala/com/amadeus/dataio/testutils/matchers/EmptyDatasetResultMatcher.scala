package com.amadeus.dataio.testutils.matchers

import org.apache.spark.sql.Dataset
import org.scalatest.matchers.{MatchResult, Matcher}

private class EmptyDatasetResultMatcher extends Matcher[Dataset[_]] {
  def apply(left: Dataset[_]): MatchResult = {
    MatchResult(
      left.isEmpty,
      "Dataset was not empty",
      "Dataset was empty"
    )
  }
}
