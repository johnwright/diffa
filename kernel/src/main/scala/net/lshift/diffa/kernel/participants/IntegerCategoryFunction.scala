/**
 * Copyright (C) 2011 LShift Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.lshift.diffa.kernel.participants

import java.lang.Integer.parseInt

abstract class IntegerCategoryFunction(denominator: Int) extends CategoryFunction {

  def shouldBucket = true

  def owningPartition(value: String) =
    try {
      denominator * (parseInt(value) / denominator) toString
    }
    catch {
      case e: NumberFormatException => throw new InvalidAttributeValueException("Value is not an integer: "+value)
    }

  def constrain(categoryName:String, partition: String) = {
    val parsedPartition = parseInt(partition)
    if (parsedPartition % denominator > 0) {
      throw new InvalidAttributeValueException("Partition "+partition+" does not match denominator "+denominator)
    }
    val start = partition
    val end = (parsedPartition + denominator - 1).toString
    RangeQueryConstraint(categoryName, Seq(start, end))
  }
}

object IntegerCategoryFunction {

  /**
   * This function partitions by groups of size `denominator`.
   * When `next` is called, it will return another with the same `factor` and with a `denominator` which is
   * `factor` times smaller.
   *
   * For example:
   *
   *     val HundredsCategoryFunction = AutoNarrowingIntegerCategoryFunction(100, 10)
   *
   */
  case class AutoNarrowingIntegerCategoryFunction(denominator: Int, factor: Int)
    extends IntegerCategoryFunction(denominator) {

      if (denominator % factor > 0) {
        throw new IllegalArgumentException(factor+" is not a factor of "+denominator)
      }

      def name = denominator.toString + "s"
      def descend = {
        val nextDenominator = denominator / factor
        if (nextDenominator <= 1) {
          Some(IndividualCategoryFunction)
        }
        else {
          Some(AutoNarrowingIntegerCategoryFunction(nextDenominator, factor))
        }
      }
  }

  /**
   * Convenience instance of AutoDescendingIntegerCategory
   */
  lazy val DefaultIntegerCategoryFunction = AutoNarrowingIntegerCategoryFunction(1000, 10)
}
