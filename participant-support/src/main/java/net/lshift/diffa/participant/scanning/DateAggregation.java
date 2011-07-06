package net.lshift.diffa.participant.scanning;

import org.joda.time.DateTime;
import org.joda.time.format.*;

/**
 * Aggregation for a date.
 */
public class DateAggregation extends AbstractScanAggregation {
  private final DateTimeParser[] parsers = new DateTimeParser[] {
          ISODateTimeFormat.dateTime().getParser(),
          ISODateTimeFormat.date().getParser()
    };
  protected final DateTimeFormatter formatter = new DateTimeFormatterBuilder().append(null, parsers).toFormatter();

  private static final DateTimeFormatter YEARLY_FORMAT = DateTimeFormat.forPattern("yyyy");
  private static final DateTimeFormatter MONTHLY_FORMAT = DateTimeFormat.forPattern("yyyy-MM");
  private static final DateTimeFormatter DAILY_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd");
  private final DateGranularityEnum granularity;


  public DateAggregation(String name, String granularity) {
    this(name, parseGranularity(granularity));
  }

  public DateAggregation(String name, DateGranularityEnum granularity) {
    super(name);

    this.granularity = granularity;
  }

  @Override
  public String bucket(String attributeVal) {
    DateTime date = null;
    try {
      date = formatter.parseDateTime(attributeVal);
    }
    catch(IllegalArgumentException e) {
      throw new InvalidAttributeValueException("Value is not a date: " + attributeVal);
    }

    switch (granularity) {
      case Daily:
        return DAILY_FORMAT.print(date);
      case Monthly:
        return MONTHLY_FORMAT.print(date);
      case Yearly:
        return YEARLY_FORMAT.print(date);
    }
    
    return null;
  }

  public DateGranularityEnum getGranularity() {
    return granularity;
  }

  public static DateGranularityEnum parseGranularity(String granStr) {
    String title =  Character.toUpperCase(granStr.charAt(0)) + granStr.substring(1).toLowerCase();
    return DateGranularityEnum.valueOf(title);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    DateAggregation that = (DateAggregation) o;

    if (granularity != that.granularity) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (granularity != null ? granularity.hashCode() : 0);
    return result;
  }
}
