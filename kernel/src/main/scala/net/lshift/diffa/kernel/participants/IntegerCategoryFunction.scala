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

case class IntegerCategoryFunction(name: String, denominator: Int, next: CategoryFunction)
  extends CategoryFunction {

  def shouldBucket = true

  def owningPartition(value: String) =
    try {
      denominator * (parseInt(value) / denominator) toString
    }
    catch {
      case e: NumberFormatException => throw new InvalidCategoryException(e)
    }

  def descend(partition: String) = {
    val start = partition
    val end = (parseInt(partition) + denominator - 1).toString
    Some(IntermediateResult(start, end, next))
  }
}

object TensCategoryFunction extends IntegerCategoryFunction("tens", 10, IndividualCategoryFunction)

object HundredsCategoryFunction extends IntegerCategoryFunction("hundreds", 100, TensCategoryFunction)

object ThousandsCategoryFunction extends IntegerCategoryFunction("thousands", 1000, HundredsCategoryFunction)
