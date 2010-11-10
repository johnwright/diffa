/**
 * Copyright (C) 2010 LShift Ltd.
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

import org.joda.time.format.DateTimeFormat
import net.lshift.diffa.kernel.differencing.DateRangeConstraint

// TODO [#2] How does this function know what argument type it can work with?
trait CategoryFunction {

  def evaluate(partition:String) : QueryAction
  def shouldBucket() : Boolean
}

case class DateCategoryFunction extends CategoryFunction {

  def evaluate(partition:String) = {
    val point = DateTimeFormat.forPattern("yyyy-MM-dd").parseDateTime(partition).toLocalDate
    val (start,end) = (point,point)
    val (s,e) = (start.toDateTimeAtStartOfDay, end.toDateTimeAtStartOfDay.plusDays(1).minusMillis(1))
    EntityQueryAction(DateRangeConstraint(s,e,DailyCategoryFunction()))
  }

  def shouldBucket() = true
}

case class DailyCategoryFunction() extends CategoryFunction {
  def evaluate(partition:String) = null
  def shouldBucket() = true
}

case class MonthlyCategoryFunction() extends CategoryFunction {
  def evaluate(partition:String) = null
  def shouldBucket() = true
}

case class YearlyCategoryFunction() extends CategoryFunction {
  def evaluate(partition:String) = null
  def shouldBucket() = true
}