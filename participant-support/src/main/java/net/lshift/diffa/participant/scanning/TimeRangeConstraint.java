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
package net.lshift.diffa.participant.scanning;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 * Constraint where a given attribute value is between a given start and end.
 */
public class TimeRangeConstraint extends AbstractScanConstraint implements RangeConstraint  {
  private static final DateTimeFormatter parser = ISODateTimeFormat.dateTimeParser();
  private static final DateTimeFormatter formatter = ISODateTimeFormat.dateTime();
  private final DateTime start;
  private final DateTime end;

  public TimeRangeConstraint(String name, String start, String end) {
    this(name, maybeParse(start), maybeParse(end));
  }
  public TimeRangeConstraint(String name, DateTime start, DateTime end) {
    super(name);

    this.start = start;
    this.end = end;
  }

  public DateTime getStart() {
    return start;
  }

  public DateTime getEnd() {
    return end;
  }

  public boolean contains(DateTime value) {
    if (start != null && value.isBefore(start)) return false;
    if (end != null && value.isAfter(end)) return false;

    return true;
  }

  @Override
  public String getStartText() {
    return start.toString(formatter);
  }

  @Override
  public String getEndText() {
    return end.toString(formatter);
  }

  private static DateTime maybeParse(String dateStr) {
    if (dateStr == null) {
      return null;
    } else {
      return parser.parseDateTime(dateStr).withZone(DateTimeZone.UTC);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    TimeRangeConstraint that = (TimeRangeConstraint) o;

    if (end != null ? !end.equals(that.end) : that.end != null) return false;
    if (start != null ? !start.equals(that.start) : that.start != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (start != null ? start.hashCode() : 0);
    result = 31 * result + (end != null ? end.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "TimeRangeConstraint{" +
        "name=" + getAttributeName() +
        ", start=" + start +
        ", end=" + end +
        '}';
  }
}
