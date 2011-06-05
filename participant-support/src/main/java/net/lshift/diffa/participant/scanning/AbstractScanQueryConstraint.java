package net.lshift.diffa.participant.scanning;

/**
 * Base implementation of a ScanQueryConstraint.
 */
public abstract class AbstractScanQueryConstraint implements ScanQueryConstraint {
  private final String name;

  public AbstractScanQueryConstraint(String name) {
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }
}
