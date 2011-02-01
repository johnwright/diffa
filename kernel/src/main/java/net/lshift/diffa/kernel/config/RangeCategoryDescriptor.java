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

public class RangeCategoryDescriptor extends CategoryDescriptor {

  public RangeCategoryDescriptor() {
  }

  public RangeCategoryDescriptor(String dataType) {
    this.dataType = dataType;
  }

  public RangeCategoryDescriptor(String lower, String upper) {
    this.lower = lower;
    this.upper = upper;
  }

  public RangeCategoryDescriptor(String dataType, String lower, String upper) {
    super(dataType);
    this.lower = lower;
    this.upper = upper;
  }

  public String lower;
  public String upper;

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

}
