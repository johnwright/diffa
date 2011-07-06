package net.lshift.diffa.participant.scanning;

/**
 * Constraint for an integer that should exist (inclusive) within a given range.
 */
public class StringPrefixConstraint extends AbstractScanConstraint {
  private final String prefix;

  public StringPrefixConstraint(String name, String prefix) {
    super(name);

    this.prefix = prefix;
  }

  public String getPrefix() {
    return prefix;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    StringPrefixConstraint that = (StringPrefixConstraint) o;

    if (prefix != null ? !prefix.equals(that.prefix) : that.prefix != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (prefix != null ? prefix.hashCode() : 0);
    return result;
  }
}
