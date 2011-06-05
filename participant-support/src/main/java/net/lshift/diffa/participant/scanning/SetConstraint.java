package net.lshift.diffa.participant.scanning;

/**
 * A constraint whereby the value of a given attribute must be within a given set of values.
 */
public class SetConstraint extends AbstractScanQueryConstraint {
  private final String[] values;

  public SetConstraint(String name, String[] values) {
    super(name);

    this.values = values;
  }

  public String[] getValues() {
    return values;
  }
}
