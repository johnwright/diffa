/**
 * Copyright (C) 2010-2011 LShift Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
