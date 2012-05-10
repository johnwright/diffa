/**
 * Copyright (C) 2010-2012 LShift Ltd.
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

public class DeleteBuilder extends SingleStatementMigrationElement {

  private final String table;
  private String where;
  private String is;


  public DeleteBuilder(String table) {
    this.table = table;
  }

  public DeleteBuilder where(String column) {
    this.where = column;
    return this;
  }

  public DeleteBuilder is(String value) {
    this.is = value;
    return this;
  }

  @Override
  protected String getSQL() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
