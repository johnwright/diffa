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

package net.lshift.diffa.docgen;

public class OptionalParameter {
  private String name;
  private String datatype;
  private String description;

    public OptionalParameter(String name, String datatype, String description) {
        this.name = name;
        this.datatype = datatype;
        this.description = description;
    }

    public String getName() {
        return name;
    }

    public String getDatatype() {
        return datatype;
    }

    public String getDescription() {
        return description;
    }
}
