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

package net.lshift.diffa.kernel.config;


/**
 * This describes a category that can be constrained by range.
 */
public class RangeCategoryDescriptor extends CategoryDescriptor {

  public RangeCategoryDescriptor() {
  }

  public RangeCategoryDescriptor(String dataType) {
    this.dataType = dataType;
  }

  public RangeCategoryDescriptor(String dataType, String lower, String upper) {
    this(dataType);
    this.lower = lower;
    this.upper = upper;
  }

  /**
   * The name of the type for attributes of this category.
   */
  public String dataType;

  /**
   * The initial lower bound which will be used for top level queries.
   */
  public String lower;

  /**
   * @param The initial upper bound which will be used for top level queries.
   */
  public String upper;

  public String getDataType() {
    return dataType;
  }

  public void setDataType(String dataType) {
    this.dataType = dataType;
  }

  public String getLower() {
    return lower;
  }

  public void setLower(String lower) {
    this.lower = lower;
  }

  public String getUpper() {
    return upper;
  }

  public void setUpper(String upper) {
    this.upper = upper;
  }

  @Override
  public String toString() {
    return "RangeCategoryDescriptor{" +
      "dataType='" + dataType + '\'' +
      ", lower='" + lower + '\'' +
      ", upper='" + upper + '\'' +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof RangeCategoryDescriptor)) return false;

    RangeCategoryDescriptor that = (RangeCategoryDescriptor) o;

    if (dataType != null ? !dataType.equals(that.dataType) : that.dataType != null) return false;
    if (lower != null ? !lower.equals(that.lower) : that.lower != null) return false;
    if (upper != null ? !upper.equals(that.upper) : that.upper != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = dataType != null ? dataType.hashCode() : 0;
    result = 31 * result + (lower != null ? lower.hashCode() : 0);
    result = 31 * result + (upper != null ? upper.hashCode() : 0);
    return result;
  }
}
