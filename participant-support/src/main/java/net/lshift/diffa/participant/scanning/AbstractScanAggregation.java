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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    AbstractScanAggregation that = (AbstractScanAggregation) o;

    if (attrName != null ? !attrName.equals(that.attrName) : that.attrName != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return attrName != null ? attrName.hashCode() : 0;
  }
}
