/**
 * Copyright (C) 2010-2011 LShift Ltd.
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
import net.lshift.diffa.participant.scanning.IntegerAggregation

/**
 * This function partitions by groups of size `denominator`.
 * When `next` is called, it will return another with the same `factor` and with a `denominator` which is
 * `factor` times smaller.
 *
 * For example:
 *
 *     val HundredsCategoryFunction = IntegerCategoryFunction(100, 10)
 */
case class IntegerCategoryFunction(attrName:String, denominator: Int, factor:Int)
  extends IntegerAggregation(attrName, denominator)
  with CategoryFunction {

  def name = denominator.toString + "s"
  def descend = {
    val nextDenominator = denominator / factor
    if (nextDenominator <= 1) {
      None
    }
    else {
      Some(IntegerCategoryFunction(attrName, nextDenominator, factor))
    }
  }

  def shouldBucket = true

  def owningPartition(value: String) =
    try {
      denominator * (parseInt(value) / denominator) toString
    }
    catch {
      case e: NumberFormatException => throw new InvalidAttributeValueException("Value is not an integer: "+value)
    }

  def constrain(constraint:QueryConstraint, partition: String) = {
    val parsedPartition = parseInt(partition)
    if (parsedPartition % denominator > 0) {
      throw new InvalidAttributeValueException("Partition "+partition+" does not match denominator "+denominator)
    }
    val start = parsedPartition
    val end = (parsedPartition + denominator - 1)
    IntegerRangeConstraint(constraint.category, start, end)
  }
}
