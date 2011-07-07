package net.lshift.diffa.participant.scanning;

/**
 * Marker interface for constraints that constrain over a range of values.
 */
public interface RangeConstraint extends ScanConstraint {

  /**
   * The start of this range as a string. The specific format will depend on the concrete implementation.
   * @return the starting value.
   */
  public String getStartText();

  /**
   * The end of this range as a string. The specific format will depend on the concrete implementation.
   * @return the ending value.
   */
  public String getEndText();

}
