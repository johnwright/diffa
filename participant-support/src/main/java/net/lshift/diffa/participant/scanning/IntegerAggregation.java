package net.lshift.diffa.participant.scanning;

/**
 * Aggregation for integers.
 */
public class IntegerAggregation extends AbstractScanAggregation {
  private final int granularity;


  public IntegerAggregation(String name, String granularity) {
    this(name, parseGranularity(granularity));
  }

  public IntegerAggregation(String name, int granularity) {
    super(name);

    this.granularity = granularity;
  }

  @Override
  public String bucket(String attributeVal) {
    int val = Integer.parseInt(attributeVal);

    return Integer.toString(granularity * (val / granularity));
  }

  public int getGranularity() {
    return granularity;
  }

  public static int parseGranularity(String granStr) {
    if (!granStr.endsWith("s")) {
      throw new IllegalArgumentException("Invalid granularity " + granStr + " - must end with s for integer granularity");
    }

    return Integer.parseInt(granStr.substring(0, granStr.length() - 1));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    IntegerAggregation that = (IntegerAggregation) o;

    if (granularity != that.granularity) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + granularity;
    return result;
  }
}
