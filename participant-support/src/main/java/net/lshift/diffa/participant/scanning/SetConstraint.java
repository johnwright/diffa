package net.lshift.diffa.participant.scanning;

import java.util.Set;

/**
 * A constraint whereby the value of a given attribute must be within a given set of values.
 */
public class SetConstraint extends AbstractScanConstraint {
  private final Set<String> values;

  public SetConstraint(String name, Set<String> values) {
    super(name);

    this.values = values;
  }

  public Set<String> getValues() {
    return values;
  }
}
