package net.lshift.diffa.participant.scanning;

/**
 * An aggregation by the name of the attribute value. Generally used when aggregating based on elements
 * out of a set.
 */
public class ByNameAggregation extends AbstractScanAggregation {
  public ByNameAggregation(String name) {
    super(name);
  }

  @Override
  public String bucket(String attributeVal) {
    return attributeVal;
  }
}
