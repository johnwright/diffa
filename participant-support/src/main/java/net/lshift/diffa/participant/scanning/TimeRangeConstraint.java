package net.lshift.diffa.participant.scanning;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 * Constraint where a given attribute value is between a given start and end.
 */
public class TimeRangeConstraint extends AbstractScanConstraint {
  private static final DateTimeFormatter formatter = ISODateTimeFormat.dateTimeParser();
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

  private static DateTime maybeParse(String dateStr) {
    if (dateStr == null) {
      return null;
    } else {
      return formatter.parseDateTime(dateStr).withZone(DateTimeZone.UTC);
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
}
