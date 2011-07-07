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

import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 * Constraint where a given attribute value is between a given start and end.
 */
public class DateRangeConstraint extends AbstractScanConstraint {
  private static final DateTimeFormatter formatter = ISODateTimeFormat.dateParser();
  private final LocalDate start;
  private final LocalDate end;

  public DateRangeConstraint(String name, String start, String end) {
    this(name, maybeParse(start), maybeParse(end));
  }
  public DateRangeConstraint(String name, LocalDate start, LocalDate end) {
    super(name);

    this.start = start;
    this.end = end;
  }

  public LocalDate getStart() {
    return start;
  }

  public LocalDate getEnd() {
    return end;
  }

  private static LocalDate maybeParse(String dateStr) {
    if (dateStr == null) {
      return null;
    } else {
      return formatter.parseDateTime(dateStr).toLocalDate();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    DateRangeConstraint that = (DateRangeConstraint) o;

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
}