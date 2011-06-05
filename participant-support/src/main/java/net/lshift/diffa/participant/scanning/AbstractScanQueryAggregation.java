package net.lshift.diffa.participant.scanning;

/**
 * Base implementation of a ScanQueryAggregation.
 */
public abstract class AbstractScanQueryAggregation implements ScanQueryAggregation {
  private final String attrName;

  public AbstractScanQueryAggregation(String attrName) {
    this.attrName = attrName;
  }

  @Override
  public String getAttributeName() {
    return attrName;
  }
}
