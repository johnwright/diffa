package net.lshift.diffa.participant.scanning;

/**
 * Describes an aggregation function being applied to a scan query.
 */
public interface ScanQueryAggregation {
  /**
   * Retrieves the name of the attribute that is being aggregated on.
   * @return the name of the aggregated attribute.
   */
  String getAttributeName();

  /**
   * Retrieves the name of the bucket that the given attribute value should be applied to.
   * @param attributeVal the attribute value
   * @return the name of the bucket that this attribute belongs to.
   */
  String bucket(String attributeVal);
}
