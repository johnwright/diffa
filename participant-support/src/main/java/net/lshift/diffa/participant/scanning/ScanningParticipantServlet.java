package net.lshift.diffa.participant.scanning;

import org.springframework.web.HttpRequestHandler;

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
public abstract class ScanningParticipantServlet
  extends HttpServlet
  implements HttpRequestHandler {

  /**
   * Delegate the Spring handleRequest implementation to the standard service dispatcher method.
   */
  @Override
  public void handleRequest(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    service(request, response);
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {

    List<ScanQueryConstraint> constraints = determineConstraints(req);
    List<ScanQueryAggregation> aggregations = determineAggregations(req);

    List<QueryResultEntry> entries = doQuery(constraints, aggregations);
    resp.setContentType("application/json");
    JSONHelper.writeQueryResult(resp.getOutputStream(), entries);
  }

  /**
   * Callback to be implemented by sub-classes to determine the constraints relevant to a request.
   * @param req the request to check for constraints.
   * @return the constraints. Default implementation is to return no constraints.
   */
  protected List<ScanQueryConstraint> determineConstraints(HttpServletRequest req) {
    return new ArrayList<ScanQueryConstraint>();
  }

  /**
   * Callback to be implemented by sub-classes to determine the aggregations relevant to a request.
   * @param req the request to check for aggregations.
   * @return the aggregations. Default implementation is to return no aggregations.
   */
  protected List<ScanQueryAggregation> determineAggregations(HttpServletRequest req) {
    return new ArrayList<ScanQueryAggregation>();
  }

  /**
   * Callback to be implemented by sub-classes to query for data relevant to this query.
   * @param constraints the constraints to apply.
   * @param aggregations the aggregations to apply.
   * @return the result entries.
   */
  protected abstract List<QueryResultEntry> doQuery(List<ScanQueryConstraint> constraints, List<ScanQueryAggregation> aggregations);
}
