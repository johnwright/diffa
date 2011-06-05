package net.lshift.diffa.participant.scanning;

/**
 * Exception indicating that a given attribute was missing from the set of attributes provided
 * for aggregation.
 */
public class MissingAttributeException extends RuntimeException {
  public MissingAttributeException(String id, String attrName) {
    super("The attribute " + attrName + " was missing for " + id);
  }
}
