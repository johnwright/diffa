package net.lshift.diffa.participant.correlation;

import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.util.Map;

/**
 * Describes a response to an uploaded entity body by a downstream participant. This allows for a correlated version
 * to be recovered by allowing the downstream to re-process content and re-generate version information.
 */
public class ProcessingResponse {
  private String id;
  private Map<String, String> attributes;
  private String uvsn;
  private String dvsn;

  public ProcessingResponse() {
  }

  public ProcessingResponse(String id, String uvsn, String dvsn) {
    this.id = id;
    this.uvsn = uvsn;
    this.dvsn = dvsn;
  }

  public ProcessingResponse(String id, Map<String, String> attributes, String uvsn, String dvsn) {
    this.id = id;
    this.attributes = attributes;
    this.uvsn = uvsn;
    this.dvsn = dvsn;
  }

  @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
  public String getId() {
    return id;
  }
  public void setId(String id) {
    this.id = id;
  }

  @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
  public Map<String, String> getAttributes() {
    return attributes;
  }
  public void setAttributes(Map<String, String> attributes) {
    this.attributes = attributes;
  }

  @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
  public String getUvsn() {
    return uvsn;
  }
  public void setUvsn(String uvsn) {
    this.uvsn = uvsn;
  }

  @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
  public String getDvsn() {
    return dvsn;
  }
  public void setDvsn(String dvsn) {
    this.dvsn = dvsn;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ProcessingResponse that = (ProcessingResponse) o;

    if (attributes != null ? !attributes.equals(that.attributes) : that.attributes != null) return false;
    if (dvsn != null ? !dvsn.equals(that.dvsn) : that.dvsn != null) return false;
    if (id != null ? !id.equals(that.id) : that.id != null) return false;
    if (uvsn != null ? !uvsn.equals(that.uvsn) : that.uvsn != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = id != null ? id.hashCode() : 0;
    result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
    result = 31 * result + (uvsn != null ? uvsn.hashCode() : 0);
    result = 31 * result + (dvsn != null ? dvsn.hashCode() : 0);
    return result;
  }
}
