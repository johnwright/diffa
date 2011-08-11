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

import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.Dialect;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Helper for describing database migrations.
 */
public class MigrationBuilder {
  private final Dialect dialect;
  private final List<MigrationElement> elements;
  private final Configuration config;

  public MigrationBuilder(Configuration config) {
    this.config = config;
    this.dialect = Dialect.getDialect(config.getProperties());
    this.elements = new ArrayList<MigrationElement>();
  }

  //
  // Builder Methods
  //

  public CreateTableBuilder createTable(String name, String...pks) {
    return register(new CreateTableBuilder(dialect, name, pks));
  }

  public InsertBuilder insert(String table) {
    return register(new InsertBuilder(dialect, table));
  }

  public AlterTableBuilder alterTable(String table) {
    return register(new AlterTableBuilder(config, dialect, table));
  }

  public DropTableBuilder dropTable(String table) {
    return register(new DropTableBuilder(config, dialect, table));
  }

  public RawSqlBuilder sql(String sql) {
    return register(new RawSqlBuilder(sql));
  }

  private <T extends MigrationElement> T register(T el) {
    elements.add(el);
    return el;
  }


  //
  // Application Methods
  //

  public void apply(Connection conn) throws SQLException {
    for (MigrationElement el : elements) {
      el.apply(conn);
    }
  }
}
