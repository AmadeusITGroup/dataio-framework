package com.amadeus.dataio.core.time

import scala.util.Try

/**
 * Collection of all time modifier parameters and constants.
 */
object Modifiers extends Enumeration {

  /**
   * Constant value is: toYear
   */
  val TruncYear = Value("toYear")

  /**
   * Constant value is: toMonth
   */
  val TruncMonth = Value("toMonth")

  /**
   * Constant value is: toDay
   */
  val TruncDay = Value("toDay")

  /**
   * Constant value is: toHour
   */
  val TruncHour = Value("toHour")

  /**
   * Constant value is: toMinute
   */
  val TruncMinute = Value("toMinute")

  /**
   * Constant value is: toSecond
   */
  val TruncSecond = Value("toSecond")

  /**
   * Constant value is: Y
   */
  val LetterYear = Value("Y")

  /**
   * Constant value is: M
   */
  val LetterMonth = Value("M")

  /**
   * Constant value is: W
   */
  val LetterWeek = Value("W")

  /**
   * Constant value is: D
   */
  val LetterDay = Value("D")

  /**
   * Constant value is: H
   */
  val LetterHour = Value("H")

  /**
   * Constant value is: m
   */
  val LetterMinute = Value("m")

  /**
   * Constant value is: s
   */
  val LetterSecond = Value("s")

  /**
   * Class that helps to make operations on a string modifier
   */
  implicit class ModifierString(modifier: String) {
    def equals(value: Value): Boolean = {
      value.toString().equals(modifier)
    }

    /**
     * Given a string modifier, it returns its corresponding constant
     * @return a modifier constant
     */
    def getModifier(): Value = {
      val value = Try(values.filter(this.equals(_)).firstKey)
      if (value.isFailure) {
        throw new IllegalArgumentException(modifier)
      } else {
        value.get
      }
    }

    /**
     * Given a string modifier, it tells if it is of truncate type
     * @return true for a truncate modifier, false for other modifiers
     */
    def isTruncateModifier(): Boolean = {
      values.foldLeft(false)((res, value) => res | this.equals(value))
    }
  }
}
