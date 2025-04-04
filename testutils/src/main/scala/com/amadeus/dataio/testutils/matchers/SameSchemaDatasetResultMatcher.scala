package com.amadeus.dataio.testutils.matchers

import org.apache.spark.sql.Dataset
import org.scalatest.matchers.{MatchResult, Matcher}

private class SameSchemaDatasetResultMatcher[T](expected: Dataset[T]) extends Matcher[Dataset[T]] {
  def apply(left: Dataset[T]): MatchResult = {
    val leftSchema  = left.schema
    val rightSchema = expected.schema

    val areEqual = leftSchema.equals(rightSchema)

    MatchResult(
      areEqual,
      s"""
         |Schemas did not match.
         |Left schema: ${leftSchema.treeString}
         |Right schema: ${rightSchema.treeString}
         |""".stripMargin,
      s"Schemas matched when they should not"
    )
  }
}
