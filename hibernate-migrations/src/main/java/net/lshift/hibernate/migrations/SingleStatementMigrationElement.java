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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Migration element that only generates a single statement.
 */
public abstract class SingleStatementMigrationElement extends TraceableMigrationElement {
  @Override
  public void apply(Connection conn) throws SQLException {
    PreparedStatement stmt = prepare(conn);
    stmt.execute();
    stmt.close();
  }

  /**
   * Prepares the element, and returns a prepared statement to execute.
   * @param conn the connection to use.
   * @return a prepared statement. Note that the caller is responsible for freeing this statement.
   */
  protected PreparedStatement prepare(Connection conn) throws SQLException {
    return prepareAndLog(conn, getSQL());
  }

  /**
   * Returns the SQL string of this single statement operation.
   * @return
   */
  protected abstract String getSQL();
}
