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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    AbstractScanConstraint that = (AbstractScanConstraint) o;

    if (attributeName != null ? !attributeName.equals(that.attributeName) : that.attributeName != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return attributeName != null ? attributeName.hashCode() : 0;
  }
}
