package net.lshift.diffa.participant.scanning;

/**
 * Base implementation of a ScanQueryConstraint.
 */
public abstract class AbstractScanQueryConstraint implements ScanQueryConstraint {
  private final String attributeName;

  public AbstractScanQueryConstraint(String attributeName) {
    this.attributeName = attributeName;
  }

  @Override
  public String getAttributeName() {
    return attributeName;
  }
}
