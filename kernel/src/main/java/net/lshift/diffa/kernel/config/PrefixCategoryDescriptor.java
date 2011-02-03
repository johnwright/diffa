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
 * This describes a category that can be constrained by a prefix.
 */
public class PrefixCategoryDescriptor extends CategoryDescriptor {

  public PrefixCategoryDescriptor() {
  }

  public PrefixCategoryDescriptor(String dataType, int prefixLength, int maxLength, int step) {
    this.dataType = dataType;
    this.prefixLength = prefixLength;
    this.maxLength = maxLength;
    this.step = step;
  }

  /**
   * The name of the type for attributes of this category.
   */
  public String dataType;

  public int prefixLength;
  public int maxLength;
  public int step;

  public String getDataType() {
    return dataType;
  }

  public void setDataType(String dataType) {
    this.dataType = dataType;
  }

  public int getPrefixLength() {
    return prefixLength;
  }

  public void setPrefixLength(int prefixLength) {
    this.prefixLength = prefixLength;
  }

  public int getMaxLength() {
    return maxLength;
  }

  public void setMaxLength(int maxLength) {
    this.maxLength = maxLength;
  }

  public int getStep() {
    return step;
  }

  public void setStep(int step) {
    this.step = step;
  }
}
