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

import java.util.Set;

/**
 * A constraint whereby the value of a given attribute must be within a given set of values.
 */
public class SetConstraint extends AbstractScanConstraint {
  private final Set<String> values;

  public SetConstraint(String name, Set<String> values) {
    super(name);

    this.values = values;
  }

  public Set<String> getValues() {
    return values;
  }
}
