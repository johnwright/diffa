package net.lshift.diffa.participant.scanning;

/**
 * Base implementation of a ScanAggregation.
 */
public abstract class AbstractScanAggregation implements ScanAggregation {
  private final String attrName;

  public AbstractScanAggregation(String attrName) {
    this.attrName = attrName;
  }

  @Override
  public String getAttributeName() {
    return attrName;
  }
}
