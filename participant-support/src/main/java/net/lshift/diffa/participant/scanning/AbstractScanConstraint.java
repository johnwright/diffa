package net.lshift.diffa.participant.scanning;

/**
 * Base implementation of a ScanConstraint.
 */
public abstract class AbstractScanConstraint implements ScanConstraint {
  private final String attributeName;

  public AbstractScanConstraint(String attributeName) {
    this.attributeName = attributeName;
  }

  @Override
  public String getAttributeName() {
    return attributeName;
  }
}
