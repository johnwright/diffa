package net.lshift.diffa.participant.scanning;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.List;

/**
 * Helper for building constraints from a web request.
 */
public class ConstraintsBuilder {
  private final HttpServletRequest req;
  private final List<ScanQueryConstraint> result;

  public ConstraintsBuilder(HttpServletRequest req) {
    this.req = req;
    this.result = new ArrayList<ScanQueryConstraint>();
  }

  /**
   * Transforms the builder into a list of constraints.
   * @return the constraint list.
   */
  public List<ScanQueryConstraint> toList() {
    return result;
  }

  /**
   * Attempt to add a date range constraint for the given attribute. The constraint will be added if
   * one or both of start-[attrName], end-[attrName] are present in the request.
   * @param attrName the name of the attribute
   */
  public void maybeAddDateRangeConstraint(String attrName) {
    String startVal = req.getParameter("start-" + attrName);
    String endVal = req.getParameter("end-" + attrName);

    if (startVal == null && endVal == null) {
      result.add(new DateRangeConstraint(attrName, startVal, endVal));
    }
  }
}
