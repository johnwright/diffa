package net.lshift.diffa.participant.scanning;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.List;

/**
 * Helper for building aggregations from a web request.
 */
public class AggregationBuilder {
  private final HttpServletRequest req;
  private final List<ScanAggregation> result;

  public AggregationBuilder(HttpServletRequest req) {
    this.req = req;
    this.result = new ArrayList<ScanAggregation>();
  }

  /**
   * Transforms the builder into a list of aggregations.
   * @return the aggregations.
   */
  public List<ScanAggregation> toList() {
    return result;
  }

  /**
   * Attempt to add a date aggregation for the given attribute. The aggregation will be added if
   * [attrName]-granularity is present in the request.
   * @param attrName the name of the attribute
   */
  public void maybeAddDateAggregation(String attrName) {
    String attrGranularity = req.getParameter(attrName + "-granularity");
    if (attrGranularity != null) {
      result.add(new DateAggregation(attrName, attrGranularity));
    }
  }

  /**
   * Attempt to add a by name aggregation for the given attribute.
   * @param attrName
   */
  public void maybeAddByNameAggregation(String attrName) {
    String attrGranularity = req.getParameter(attrName + "-granularity");
    if (attrGranularity != null && attrGranularity.equals("by-name")) {
      result.add(new ByNameAggregation(attrName));
    }
  }

  /**
   * Attempt to add a integer aggregation for the given attribute. The aggregation will be added if
   * [attrName]-granularity is present in the request.
   * @param attrName the name of the attribute
   */
  public void maybeAddIntegerAggregation(String attrName) {
    String attrGranularity = req.getParameter(attrName + "-granularity");
    if (attrGranularity != null) {
      result.add(new IntegerAggregation(attrName, attrGranularity));
    }
  }
}
