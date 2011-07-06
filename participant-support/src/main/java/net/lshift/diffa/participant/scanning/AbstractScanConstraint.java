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
 * Base implementation of a ScanConstraint.
 */
public abstract class AbstractScanConstraint implements ScanConstraint {
  private final String attributeName;

  public AbstractScanConstraint(String attributeName) {
    this.attributeName = attributeName;
  }

  @Override
  public String getAttributeName() {
    return attributeName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    AbstractScanConstraint that = (AbstractScanConstraint) o;

    if (attributeName != null ? !attributeName.equals(that.attributeName) : that.attributeName != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return attributeName != null ? attributeName.hashCode() : 0;
  }
}
