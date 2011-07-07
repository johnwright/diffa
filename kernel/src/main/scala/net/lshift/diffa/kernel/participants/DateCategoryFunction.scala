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
import net.lshift.diffa.participant.scanning._

/**
 * Extension of the DateCategoryFunction to support internal functions.
 */
abstract class DateCategoryFunction(attrName:String, dataType:DateCategoryDataType, granularity:DateGranularityEnum)
  extends DateAggregation(attrName, granularity)
  with CategoryFunction {

  def pattern:DateTimeFormatter
  def descend:Option[CategoryFunction]
  def pointToBounds(d:LocalDate) : (LocalDate,LocalDate)

  def constrain(partition:String) = {
    val point = pattern.parseDateTime(partition).toLocalDate
    val (lower,upper) = pointToBounds(point)

    dataType match {
      case DateDataType => new DateRangeConstraint(attrName, lower, upper)
      case TimeDataType => new TimeRangeConstraint(attrName, sod(lower), eod(upper))
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
 * Typed indicator for whether a date category refers to just a date or a full date time.
 */
abstract sealed class DateCategoryDataType
case object DateDataType extends DateCategoryDataType
case object TimeDataType extends DateCategoryDataType

/**
 * This function partitions by whole days.
 */
case class DailyCategoryFunction(attrName:String, dataType:DateCategoryDataType) extends DateCategoryFunction(attrName, dataType, DateGranularityEnum.Daily) {
  def name = "daily"
  def pattern = DateTimeFormat.forPattern("yyyy-MM-dd")
  def descend = None
  def pointToBounds(point:LocalDate) = (point,point)
}

/**
 * This function partitions by whole calendar months.
 */
case class MonthlyCategoryFunction(attrName:String, dataType:DateCategoryDataType) extends DateCategoryFunction(attrName, dataType, DateGranularityEnum.Monthly) {
  def name = "monthly"
  def pattern = DateTimeFormat.forPattern("yyyy-MM")
  def descend = Some(DailyCategoryFunction(attrName, dataType))
  def pointToBounds(point:LocalDate) = (point.withDayOfMonth(1), point.plusMonths(1).minusDays(1))
}

/**
 * This function partitions by whole years.
 */
case class YearlyCategoryFunction(attrName:String, dataType:DateCategoryDataType) extends DateCategoryFunction(attrName, dataType, DateGranularityEnum.Yearly) {
  def name = "yearly"
  def pattern = DateTimeFormat.forPattern("yyyy")
  def descend = Some(MonthlyCategoryFunction(attrName, dataType))
  def pointToBounds(point:LocalDate) = (point.withMonthOfYear(1).withDayOfMonth(1), point.withMonthOfYear(12).withDayOfMonth(31))
}