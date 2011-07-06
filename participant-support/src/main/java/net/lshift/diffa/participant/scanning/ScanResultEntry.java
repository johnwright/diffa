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

import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.joda.time.DateTime;

import java.util.Map;

/**
 * Describes an element of the response to a scanning query. Can either describe a specific entity, or an aggregate of
 * entities. Aggregates do not contain an id or lastUpdated value.
 */
public class ScanResultEntry {
  private String id;
  private String version;
  private DateTime lastUpdated;
  private Map<String, String> attributes;

  public static ScanResultEntry forAggregate(String version, Map<String, String> attributes) {
    return new ScanResultEntry(null, version, null, attributes);
  }
  public static ScanResultEntry forEntity(String id, String version, DateTime lastUpdated, Map<String, String> attributes) {
    return new ScanResultEntry(id, version, lastUpdated, attributes);
  }
  public static ScanResultEntry forEntity(String id, String version, DateTime lastUpdated) {
    return new ScanResultEntry(id, version, lastUpdated, null);
  }

  public ScanResultEntry() {
    this(null, null, null, null);
  }

  public ScanResultEntry(String id, String version, DateTime lastUpdated, Map<String, String> attributes) {
    this.id = id;
    this.version = version;
    this.lastUpdated = lastUpdated;
    this.attributes = attributes;
  }

  @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
  public String getId() {
    return id;
  }
  public void setId(String id) {
    this.id = id;
  }

  @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
  public String getVersion() {
    return version;
  }
  public void setVersion(String version) {
    this.version = version;
  }

  @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
  public DateTime getLastUpdated() {
    return lastUpdated;
  }
  public void setLastUpdated(DateTime lastUpdated) {
    this.lastUpdated = lastUpdated;
  }

  @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
  public Map<String, String> getAttributes() {
    return attributes;
  }
  public void setAttributes(Map<String, String> attributes) {
    this.attributes = attributes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ScanResultEntry that = (ScanResultEntry) o;

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
    return "ScanResultEntry{" +
      "id='" + id + '\'' +
      ", version='" + version + '\'' +
      ", lastUpdated=" + lastUpdated +
      ", attributes=" + attributes +
      '}';
  }
}
