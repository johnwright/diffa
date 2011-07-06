package net.lshift.diffa.participant.scanning;

import javax.servlet.http.HttpServletRequest;
import java.util.List;

/**
 * Handler interface that can be implemented in client libraries to allow for scanning.
 */

public interface ScanningParticipantHandler {
  /**
   * Callback to be implemented by sub-classes to determine the constraints relevant to a request.
   * @param req the request to check for constraints.
   * @return the constraints. Default implementation is to return no constraints.
   */
  List<ScanConstraint> determineConstraints(HttpServletRequest req);

  /**
   * Callback to be implemented by sub-classes to determine the aggregations relevant to a request.
   * @param req the request to check for aggregations.
   * @return the aggregations. Default implementation is to return no aggregations.
   */
  List<ScanAggregation> determineAggregations(HttpServletRequest req);

  /**
   * Callback to be implemented by sub-classes to query for data relevant to this query.
   * @param constraints the constraints to apply.
   * @param aggregations the aggregations to apply.
   * @return the result entries.
   */
  List<ScanResultEntry> doQuery(List<ScanConstraint> constraints, List<ScanAggregation> aggregations);
}
