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

abstract case class IntegerCategoryFunction(denominator: Int) extends CategoryFunction {

  def name: String
  def next: CategoryFunction

  def shouldBucket = true

  def owningPartition(value: String) =
    try {
      denominator * (parseInt(value) / denominator) toString
    }
    catch {
      case e: NumberFormatException => throw new InvalidCategoryException(e)
    }

  def descend(partition: String) = {
    val parsedPartition = parseInt(partition)
    if (parsedPartition % denominator > 0)
      // TODO formalise this with its own exception type
      throw new RuntimeException("Partition "+partition+" does not match denominator "+denominator)
    val start = partition
    val end = (parsedPartition + denominator - 1).toString
    Some(IntermediateResult(start, end, next))
  }
}

object IntegerCategoryFunction {
  def AutoDescendingIntegerCategoryFunction(denominator: Int, factor: Int): IntegerCategoryFunction =
    new IntegerCategoryFunction(denominator) {
      def name = denominator.toString + "s"
      def next = {
        val nextDenominator = denominator / factor
        if (nextDenominator <= 1)
          IndividualCategoryFunction
        else
          AutoDescendingIntegerCategoryFunction(denominator/factor, factor)
      }
    }

  def TensCategoryFunction = AutoDescendingIntegerCategoryFunction(10, 10)
  def HundredsCategoryFunction = AutoDescendingIntegerCategoryFunction(100, 10)
  def ThousandsCategoryFunction = AutoDescendingIntegerCategoryFunction(1000, 10)
}
