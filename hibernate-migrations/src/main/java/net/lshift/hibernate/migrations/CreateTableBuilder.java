/**
 * Copyright (C) 2011 LShift Ltd.
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
import org.hibernate.mapping.Column;
import org.hibernate.mapping.PrimaryKey;

import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static net.lshift.hibernate.migrations.SQLStringHelpers.generateColumnString;
import static net.lshift.hibernate.migrations.SQLStringHelpers.generateIdentityColumnString;

/**
 * Describes a table that is to be created.
 */
public class CreateTableBuilder extends SingleStatementMigrationElement {
  private final Dialect dialect;
  private final String name;
  private final List<String> primaryKeys;
  private final List<Column> columns;
  private boolean identityCol = false;

  public CreateTableBuilder(Dialect dialect, String name, String... primaryKeys) {
    this.dialect = dialect;
    this.name = name;
    this.primaryKeys = new ArrayList<String>(Arrays.asList(primaryKeys));
    this.columns = new ArrayList<Column>();
  }

  //
  // Builder Methods
  //

  public CreateTableBuilder withIdentityCol() {
    identityCol = true;
    return this;
  }

  public CreateTableBuilder pk(String...names) {
    primaryKeys.addAll(Arrays.asList(names));
    return this;
  }

  public CreateTableBuilder column(String name, int sqlType, boolean nullable) {
    return column(name, sqlType, Column.DEFAULT_LENGTH, nullable);
  }

  public CreateTableBuilder column(String name, int sqlType, int length, boolean nullable) {
    Column col = new Column(name);
    col.setNullable(nullable);
    col.setSqlTypeCode(sqlType);
    col.setLength(length);
    columns.add(col);

    return this;
  }


  //
  // Output Methods
  //


  @Override
  protected PreparedStatement prepare(Connection conn) throws SQLException {
    return conn.prepareStatement(toSql());
  }

  public String toSql() {
    StringBuffer buffer =
        new StringBuffer(dialect.getCreateTableString()).append(' ').append(dialect.quote(name)).append(" (");

    for (Column col : columns) {
      if (identityCol && primaryKeys.contains(col.getName())) {
        buffer.append(generateIdentityColumnString(dialect, col));
      } else {
        buffer.append(generateColumnString(dialect, col, true));
      }
      buffer.append(", ");
    }
    buffer.append(getPrimaryKey().sqlConstraintString(dialect));

    buffer.append(")");
    return buffer.toString();
  }

  public PrimaryKey getPrimaryKey() {
    PrimaryKey result = new PrimaryKey();
    for (String pk : primaryKeys) result.addColumn(new Column(pk));
    return result;
  }
}