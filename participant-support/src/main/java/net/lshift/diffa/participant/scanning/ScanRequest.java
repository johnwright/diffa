package net.lshift.diffa.participant.scanning;

import java.util.List;

/**
 * Describe a request to perform a specific scan.
 */
public class ScanRequest {
  private final List<ScanConstraint> constraints;
  private final List<ScanAggregation> aggregations;

  public ScanRequest(List<ScanConstraint> constraints, List<ScanAggregation> aggregations) {
    this.constraints = constraints;
    this.aggregations = aggregations;
  }

  public List<ScanConstraint> getConstraints() {
    return constraints;
  }

  public List<ScanAggregation> getAggregations() {
    return aggregations;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ScanRequest that = (ScanRequest) o;

    if (aggregations != null ? !aggregations.equals(that.aggregations) : that.aggregations != null) return false;
    if (constraints != null ? !constraints.equals(that.constraints) : that.constraints != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = constraints != null ? constraints.hashCode() : 0;
    result = 31 * result + (aggregations != null ? aggregations.hashCode() : 0);
    return result;
  }
}
