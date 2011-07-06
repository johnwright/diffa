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

/**
 * Aggregation for strings by prefix.
 */
public class StringPrefixAggregation extends AbstractScanAggregation {
  private final int length;


  public StringPrefixAggregation(String name, String length) {
    this(name, parseGranularity(length));
  }

  public StringPrefixAggregation(String name, int length) {
    super(name);

    this.length = length;
  }

  @Override
  public String bucket(String attributeVal) {
    if (attributeVal.length() <= length) return attributeVal;
    return attributeVal.substring(0, length);
  }

  public int getLength() {
    return length;
  }

  public static int parseGranularity(String lengthStr) {
    return Integer.parseInt(lengthStr);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    StringPrefixAggregation that = (StringPrefixAggregation) o;

    if (length != that.length) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + length;
    return result;
  }
}
