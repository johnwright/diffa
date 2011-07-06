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

import org.joda.time.LocalDate
import org.joda.time.format.{DateTimeFormatter, DateTimeFormat}
import net.lshift.diffa.kernel.config.{DateTypeDescriptor,DateTimeTypeDescriptor}
import net.lshift.diffa.participant.scanning.{DateGranularityEnum, DateAggregation}

/**
 * Extension of the DateCategoryFunction to support internal functions.
 */
abstract class DateCategoryFunction(attrName:String, granularity:DateGranularityEnum)
  extends DateAggregation(attrName, granularity)
  with CategoryFunction {

  def pattern:DateTimeFormatter
  def descend:Option[CategoryFunction]
  def pointToBounds(d:LocalDate) : (LocalDate,LocalDate)

  def constrain(parent:QueryConstraint, partition:String) = {
    val point = pattern.parseDateTime(partition).toLocalDate
    val (lower,upper) = pointToBounds(point)

    parent match {
      case d:DateRangeConstraint
        if d.start != null && d.end != null
            && (d.start.isAfter(lower) || d.end.isBefore(upper)) => d
      case t:DateTimeRangeConstraint
        if t.start != null && t.end != null
            && (t.start.isAfter(sod(lower)) || t.end.isBefore(eod(upper))) => t
      case _ =>
         parent.dataType match {
          case DateTypeDescriptor     => new DateRangeConstraint(parent.category, lower, upper)
          case DateTimeTypeDescriptor => new DateTimeRangeConstraint(parent.category, sod(lower), eod(upper))
        }
    }
  }

  /**
   * Convert to a DateTime using zero o'clock, i.e. the start of the day
   */
  def sod(d:LocalDate) = d.toDateTimeAtStartOfDay

  /**
   * Convert to a DateTime using a millisecond before the next day, i.e. the end of the day
   */
  def eod(d:LocalDate) = d.toDateTimeAtStartOfDay.plusDays(1).minusMillis(1)

  def shouldBucket() = true
}

/**
 * This function partitions by whole days.
 */
case class DailyCategoryFunction(attrName:String) extends DateCategoryFunction(attrName, DateGranularityEnum.Daily) {
  def name = "daily"
  def pattern = DateTimeFormat.forPattern("yyyy-MM-dd")
  def descend = None
  def pointToBounds(point:LocalDate) = (point,point)
}

/**
 * This function partitions by whole calendar months.
 */
case class MonthlyCategoryFunction(attrName:String) extends DateCategoryFunction(attrName, DateGranularityEnum.Monthly) {
  def name = "monthly"
  def pattern = DateTimeFormat.forPattern("yyyy-MM")
  def descend = Some(DailyCategoryFunction(attrName))
  def pointToBounds(point:LocalDate) = (point.withDayOfMonth(1), point.plusMonths(1).minusDays(1))
}

/**
 * This function partitions by whole years.
 */
case class YearlyCategoryFunction(attrName:String) extends DateCategoryFunction(attrName, DateGranularityEnum.Yearly) {
  def name = "yearly"
  def pattern = DateTimeFormat.forPattern("yyyy")
  def descend = Some(MonthlyCategoryFunction(attrName))
  def pointToBounds(point:LocalDate) = (point.withMonthOfYear(1).withDayOfMonth(1), point.withMonthOfYear(12).withDayOfMonth(31))
}