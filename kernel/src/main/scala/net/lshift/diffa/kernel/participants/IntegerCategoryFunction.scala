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

abstract case class IntegerCategoryFunction extends CategoryFunction {
  val denominator: Int
  def next: CategoryFunction

  def shouldBucket = true

  def owningPartition(value: String) =
    try {
      parseInt(value) / denominator toString
    }
    catch {
      case e: NumberFormatException => throw new InvalidCategoryException(e)
    }

  def descend(partition: String) = {
    // TODO
    val start = "0"
    val end = "1"
    Some(IntermediateResult(start, end, next))
  }
}

object TensCategoryFunction extends IntegerCategoryFunction {
  def name = "tens"
  val denominator = 10
  def next = IndividualCategoryFunction
}

object HundredsCategoryFunction extends IntegerCategoryFunction {
  def name = "hundreds"
  val denominator = 100
  def next = TensCategoryFunction
}

object ThousandsCategoryFunction extends IntegerCategoryFunction {
  def name = "thousands"
  val denominator = 1000
  def next = HundredsCategoryFunction
}
