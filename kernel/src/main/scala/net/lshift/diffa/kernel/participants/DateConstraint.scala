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

import org.joda.time.{Interval, DateTime}

case class DateConstraint(start:DateTime, end:DateTime, f:CategoryFunction)
  extends BaseQueryConstraint("date", f, Seq(start.toString(), end.toString()))

case class SimpleDateConstraint(override val start:DateTime, override val end:DateTime) extends DateConstraint(start, end, DailyCategoryFunction()) {


  @Deprecated
  private val interval = if (start != null && end != null) {
    new Interval(start, end)
  } else {
    null
  }
  
  @Deprecated
  def contains(d: DateTime): Boolean = {
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
}