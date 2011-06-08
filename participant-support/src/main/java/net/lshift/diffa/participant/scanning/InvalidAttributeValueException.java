package net.lshift.diffa.participant.scanning;

/**
 * Exception indicating that an attribute value was in an un-acceptible for a given aggregation.
 */
public class InvalidAttributeValueException extends RuntimeException {
  public InvalidAttributeValueException(String s) {
    super(s);
  }
}
