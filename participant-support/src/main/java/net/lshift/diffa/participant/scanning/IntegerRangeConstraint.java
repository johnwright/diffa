package net.lshift.diffa.participant.scanning;

/**
 * Constraint for an integer that should exist (inclusive) within a given range.
 */
public class IntegerRangeConstraint extends AbstractScanConstraint {
  private final Integer start;
  private final Integer end;

  public IntegerRangeConstraint(String name, String start, String end) {
    this(name, maybeParse(start), maybeParse(end));
  }
  public IntegerRangeConstraint(String name, Integer start, Integer end) {
    super(name);

    this.start = start;
    this.end = end;
  }

  public Integer getStart() {
    return start;
  }

  public Integer getEnd() {
    return end;
  }

  private static Integer maybeParse(String intStr) {
    if (intStr == null) {
      return null;
    } else {
      return Integer.parseInt(intStr);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    IntegerRangeConstraint that = (IntegerRangeConstraint) o;

    if (end != null ? !end.equals(that.end) : that.end != null) return false;
    if (start != null ? !start.equals(that.start) : that.start != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (start != null ? start.hashCode() : 0);
    result = 31 * result + (end != null ? end.hashCode() : 0);
    return result;
  }
}
