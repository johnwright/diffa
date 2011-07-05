package net.lshift.diffa.participant.scanning;

import net.lshift.diffa.participant.common.JSONHelper;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Base support servlet for implementing a Diffa scanning participant.
 */
public abstract class ScanningParticipantServlet extends HttpServlet {
  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {

    List<ScanConstraint> constraints = determineConstraints(req);
    List<ScanAggregation> aggregations = determineAggregations(req);

    List<ScanResultEntry> entries = doQuery(constraints, aggregations);
    resp.setContentType("application/json");
    JSONHelper.writeQueryResult(resp.getOutputStream(), entries);
  }

  /**
   * Callback to be implemented by sub-classes to determine the constraints relevant to a request.
   * @param req the request to check for constraints.
   * @return the constraints. Default implementation is to return no constraints.
   */
  protected List<ScanConstraint> determineConstraints(HttpServletRequest req) {
    return new ArrayList<ScanConstraint>();
  }

  /**
   * Callback to be implemented by sub-classes to determine the aggregations relevant to a request.
   * @param req the request to check for aggregations.
   * @return the aggregations. Default implementation is to return no aggregations.
   */
  protected List<ScanAggregation> determineAggregations(HttpServletRequest req) {
    return new ArrayList<ScanAggregation>();
  }

  /**
   * Callback to be implemented by sub-classes to query for data relevant to this query.
   * @param constraints the constraints to apply.
   * @param aggregations the aggregations to apply.
   * @return the result entries.
   */
  protected abstract List<ScanResultEntry> doQuery(List<ScanConstraint> constraints, List<ScanAggregation> aggregations);
}
