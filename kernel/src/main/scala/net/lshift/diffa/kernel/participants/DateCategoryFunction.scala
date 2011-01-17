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

import org.joda.time.LocalDate
import org.joda.time.format.{ISODateTimeFormat, DateTimeFormatter, DateTimeFormat}

abstract case class DateCategoryFunction extends CategoryFunction {

  protected val isoFormat = ISODateTimeFormat.dateTime()

  def pattern:DateTimeFormatter
  def next:CategoryFunction
  def pointToBounds(d:LocalDate) : (LocalDate,LocalDate)

  def descend(partition:String) = {
    val point = pattern.parseDateTime(partition).toLocalDate
    val (upper,lower) = pointToBounds(point)
    val (start,end) = align(upper,lower)
    Some(IntermediateResult(start,end, next))
  }

  def align(s:LocalDate, e:LocalDate) = (s.toDateTimeAtStartOfDay, e.toDateTimeAtStartOfDay.plusDays(1).minusMillis(1))

  def shouldBucket() = true

  override def owningPartition(value:String) =
    try {
      val date = isoFormat.parseDateTime(value)
      pattern.print(date)
    }
    catch {
      case e: IllegalArgumentException => throw new InvalidAttributeValueException("Value is not a date: "+value)
    }
}

/**
 * This function partitions by whole days.
 */
object DailyCategoryFunction extends DateCategoryFunction {
  def name = "daily"
  def pattern = DateTimeFormat.forPattern("yyyy-MM-dd")
  def next = IndividualCategoryFunction
  def pointToBounds(point:LocalDate) = (point,point)
}

/**
 * This function partitions by whole calendar months.
 */
object MonthlyCategoryFunction extends DateCategoryFunction {
  def name = "monthly"
  def pattern = DateTimeFormat.forPattern("yyyy-MM")
  def next = DailyCategoryFunction
  def pointToBounds(point:LocalDate) = (point.withDayOfMonth(1), point.plusMonths(1).minusDays(1))
}

/**
 * This function partitions by whole years.
 */
object YearlyCategoryFunction extends DateCategoryFunction {
  def name = "yearly"
  def pattern = DateTimeFormat.forPattern("yyyy")
  def next = MonthlyCategoryFunction
  def pointToBounds(point:LocalDate) = (point.withMonthOfYear(1).withDayOfMonth(1), point.withMonthOfYear(12).withDayOfMonth(31))
}