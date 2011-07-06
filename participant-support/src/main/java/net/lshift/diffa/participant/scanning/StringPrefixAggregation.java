package net.lshift.diffa.participant.scanning;

/**
 * Aggregation for strings by prefix.
 */
public class StringPrefixAggregation extends AbstractScanAggregation {
  private final int length;


  public StringPrefixAggregation(String name, String length) {
    this(name, parseGranularity(length));
  }

  public StringPrefixAggregation(String name, int length) {
    super(name);

    this.length = length;
  }

  @Override
  public String bucket(String attributeVal) {
    if (attributeVal.length() <= length) return attributeVal;
    return attributeVal.substring(0, length);
  }

  public int getLength() {
    return length;
  }

  public static int parseGranularity(String lengthStr) {
    return Integer.parseInt(lengthStr);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    StringPrefixAggregation that = (StringPrefixAggregation) o;

    if (length != that.length) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + length;
    return result;
  }
}
