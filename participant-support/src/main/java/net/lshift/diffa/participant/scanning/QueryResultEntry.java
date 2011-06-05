package net.lshift.diffa.participant.scanning;

import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.joda.time.DateTime;

import java.util.Map;

/**
 * Describes an element of the response to a scanning query. Can either describe a specific entity, or an aggregate of
 * entities. Aggregates do not contain an id or lastUpdated value.
 */
public class QueryResultEntry {
  private final String id;
  private final String version;
  private final DateTime lastUpdated;
  private final Map<String, String> attributes;

  public static QueryResultEntry forAggregate(String version, Map<String, String> attributes) {
    return new QueryResultEntry(null, version, null, attributes);
  }
  public static QueryResultEntry forEntity(String id, String version, DateTime lastUpdated, Map<String, String> attributes) {
    return new QueryResultEntry(id, version, lastUpdated, attributes);
  }
  public static QueryResultEntry forEntity(String id, String version, DateTime lastUpdated) {
    return new QueryResultEntry(id, version, lastUpdated, null);
  }

  public QueryResultEntry() {
    this(null, null, null, null);
  }

  public QueryResultEntry(String id, String version, DateTime lastUpdated, Map<String, String> attributes) {
    this.id = id;
    this.version = version;
    this.lastUpdated = lastUpdated;
    this.attributes = attributes;
  }

  @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
  public String getId() {
    return id;
  }

  @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
  public String getVersion() {
    return version;
  }

  @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
  public DateTime getLastUpdated() {
    return lastUpdated;
  }

  @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
  public Map<String, String> getAttributes() {
    return attributes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    QueryResultEntry that = (QueryResultEntry) o;

    if (attributes != null ? !attributes.equals(that.attributes) : that.attributes != null) return false;
    if (id != null ? !id.equals(that.id) : that.id != null) return false;
    if (lastUpdated != null ? !lastUpdated.equals(that.lastUpdated) : that.lastUpdated != null) return false;
    if (version != null ? !version.equals(that.version) : that.version != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = id != null ? id.hashCode() : 0;
    result = 31 * result + (version != null ? version.hashCode() : 0);
    result = 31 * result + (lastUpdated != null ? lastUpdated.hashCode() : 0);
    result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "QueryResultEntry{" +
      "id='" + id + '\'' +
      ", version='" + version + '\'' +
      ", lastUpdated=" + lastUpdated +
      ", attributes=" + attributes +
      '}';
  }
}
