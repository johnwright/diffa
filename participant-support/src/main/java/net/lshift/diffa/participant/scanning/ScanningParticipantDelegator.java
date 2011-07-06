package net.lshift.diffa.participant.scanning;

import javax.servlet.http.HttpServletRequest;
import java.util.List;

/**
 * Adapter allowing a ScanningParticipant to be implemented without requiring it to sub-class the
 * ScanningParticipantRequestHandler or the ScanningParticipantServlet, and instead be delegated to.
 */
public class ScanningParticipantDelegator extends ScanningParticipantRequestHandler {
  private final ScanningParticipantHandler handler;

  public ScanningParticipantDelegator(ScanningParticipantHandler handler) {
    this.handler = handler;
  }

  @Override
  protected List<ScanConstraint> determineConstraints(HttpServletRequest req) {
    return handler.determineConstraints(req);
  }

  @Override
  protected List<ScanAggregation> determineAggregations(HttpServletRequest req) {
    return handler.determineAggregations(req);
  }

  @Override
  protected List<ScanResultEntry> doQuery(List<ScanConstraint> constraints, List<ScanAggregation> aggregations) {
    return handler.doQuery(constraints, aggregations);
  }
}
