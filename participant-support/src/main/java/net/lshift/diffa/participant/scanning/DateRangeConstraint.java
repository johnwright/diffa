package net.lshift.diffa.participant.scanning;

import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 * Constraint where a given attribute value is between a given start and end.
 */
public class DateRangeConstraint extends AbstractScanQueryConstraint {
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
}
