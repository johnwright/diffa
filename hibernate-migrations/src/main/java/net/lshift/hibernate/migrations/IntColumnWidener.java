/**
 * Copyright (C) 201-2012 LShift Ltd.
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

import net.lshift.hibernate.migrations.dialects.DialectExtension;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class IntColumnWidener implements MigrationElement {

  private final DialectExtension dialectExtension;
  private final String table;
  private String column = null;
  private List<String> statements = null;

  public IntColumnWidener(DialectExtension dialectExtension, String table) {
    this.dialectExtension = dialectExtension;
    this.table = table;
  }

  public IntColumnWidener column(String column) {
    this.column = column;
    this.statements = dialectExtension.widenIntColumn(table, column);
    return this;
  }

  @Override
  public void apply(Connection conn) throws SQLException {
    for (String sql : statements) {
      PreparedStatement stmt = conn.prepareStatement(sql);
      stmt.execute();
      stmt.close();
    }
  }

  @Override
  public List<String> getStatements() {
    return statements;
  }
}
