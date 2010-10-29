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

package net.lshift.diffa.kernel.differencing

import org.joda.time.{Interval, DateTime}
import net.lshift.diffa.kernel.participants.RangeQueryConstraint

/**
 *  Selects an entity based on whether it exists between a given start and end date.
 */
case class DateRangeConstraint(start:DateTime, end:DateTime) extends RangeQueryConstraint {

  def category = "date"
  def function = "DATE_RANGE"
  def values = List[String](start.toString, end.toString)

  private val interval = if (start != null && end != null) {
      new Interval(start, end)
    } else {
      null
    }

  /**
   * Determine whether the selector contains the given date. This will return true if the date is between
   * the start and the end date; or if the date is after the start when the end is null; or if the date is
   * before the end if start is null.
   */
  def contains(d:DateTime):Boolean = {
    if (interval == null) {
      if (start == null && end == null) {
        true
      } else if (start == null) {
        !end.isBefore(d)
      } else {
        !start.isAfter(d)
      }
    } else {
      interval.contains(d)
    }
  }

  /**
   * Intersect the selector with the given date range
   */
  def constrain(other:DateRangeConstraint):DateRangeConstraint = {
    val lowerBound = (other.start, this.start) match {
      case (null, _) => this.start
      case (_, null) => other.start
      case _ => if (this.start.compareTo(other.start) < 0) other.start else this.start
    }
    val upperBound = (other.end, this.end) match {
      case (null, _) => this.end
      case (_, null) => other.end
      case _ => if (other.end.compareTo(this.end) < 0) other.end else this.end
    }
    DateRangeConstraint(lowerBound, upperBound)
  }
}

object DateRangeConstraint {
  /**
   * Starting point date constraint that accepts any date (ie, it has no bounds).
   */
  val any = DateRangeConstraint(null, null)
}