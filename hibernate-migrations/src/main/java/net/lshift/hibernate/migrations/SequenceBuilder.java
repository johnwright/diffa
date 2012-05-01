/**
 * Copyright (C) 2012 LShift Ltd.
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
package net.lshift.hibernate.migrations;

import org.hibernate.dialect.Dialect;

public class SequenceBuilder extends SingleStatementMigrationElement {

  private final String sequenceName;

  private int increment = Integer.MIN_VALUE;
  private int startWith = Integer.MIN_VALUE;

  public SequenceBuilder(String sequenceName) {
    this.sequenceName = sequenceName;
  }

  @Override
  protected String getSQL() {
    String sql = "create sequence " + sequenceName;

    if (increment > Integer.MIN_VALUE) {
      sql = sql + " increment by " + increment;
    }

    if (startWith > Integer.MIN_VALUE) {
      sql = sql + " start with " + startWith;
    }

    return sql;
  }

  public SequenceBuilder incrementBy(int inc) {
    if (inc < 1) {
      throw new IllegalArgumentException("Cannot increment by " + inc);
    }
    this.increment = inc;
    return this;
  }

  public SequenceBuilder startWith(int start) {
    if (start < 0) {
      throw new IllegalArgumentException("Cannot start with " + start);
    }
    this.startWith = start;
    return this;
  }
}
