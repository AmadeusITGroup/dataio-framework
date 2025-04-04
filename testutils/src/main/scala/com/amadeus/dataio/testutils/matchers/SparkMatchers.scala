package com.amadeus.dataio.testutils.matchers

import org.apache.spark.sql._
import org.scalatest.matchers.Matcher
import org.scalatest.matchers.should.Matchers

trait SparkMatchers extends Matchers {

  /** Matches when a Dataset is empty (has 0 rows). */
  def beEmpty: Matcher[Dataset[_]] = new EmptyDatasetResultMatcher

  /** Matches when a Dataset is not empty (has at least 1 row). */
  def notBeEmpty: Matcher[Dataset[_]] = new NotEmptyDatasetResultMatcher

  /** Matches when a Dataset has exactly the specified number of rows. */
  def haveCountOf(expected: Long): Matcher[Dataset[_]] = new CountDatasetResultMatcher(expected)

  /** Matches when a Dataset has the same rows as the expected Dataset. */
  def containTheSameRowsAs[T](expected: Dataset[T]): Matcher[Dataset[T]] = new SameRowsDatasetResultMatcher[T](expected)

  /** Matches when a Dataset has exactly the same schema as the expected Dataset schema. */
  def haveTheSameSchemaAs[T](expected: Dataset[T]): Matcher[Dataset[T]] = new SameSchemaDatasetResultMatcher[T](expected)
}
