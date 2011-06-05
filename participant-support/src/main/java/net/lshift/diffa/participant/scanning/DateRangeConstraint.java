package net.lshift.diffa.participant.scanning;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 * Constraint where a given attribute value is between a given start and end.
 */
public class DateRangeConstraint extends AbstractScanQueryConstraint {
  private static final DateTimeFormatter formatter = ISODateTimeFormat.dateTimeParser();
  private final DateTime start;
  private final DateTime end;

  public DateRangeConstraint(String name, String start, String end) {
    this(name, maybeParse(start), maybeParse(end));
  }
  public DateRangeConstraint(String name, DateTime start, DateTime end) {
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
      return formatter.parseDateTime(dateStr);
    }
  }
}
