package com.amadeus.dataio.testutils.matchers

import org.apache.spark.sql.Dataset
import org.scalatest.matchers.{MatchResult, Matcher}

private class SameRowsDatasetResultMatcher[T](expected: Dataset[T]) extends Matcher[Dataset[T]] {
  def apply(left: Dataset[T]): MatchResult = {
    val leftExceptRight = left.except(expected)
    val rightExceptLeft = expected.except(left)
    val areEqual        = leftExceptRight.isEmpty && rightExceptLeft.isEmpty

    MatchResult(
      areEqual,
      s"""
         |Datasets did not contain the same rows.
         |Unexpected rows: ${if (leftExceptRight.isEmpty) "none"
      else leftExceptRight.toJSON.take(5).map(s => s"\n\t $s").mkString("")}
         |Missing expected rows: ${if (rightExceptLeft.isEmpty) "none"
      else rightExceptLeft.toJSON.take(5).map(s => s"\n\t $s").mkString("")}
         |""".stripMargin,
      s"Datasets contained the same rows when they should not"
    )
  }
}
