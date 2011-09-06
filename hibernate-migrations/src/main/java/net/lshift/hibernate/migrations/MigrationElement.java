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
import java.util.List;

/**
 * Describes an element that can be executed within a migration.
 */
public interface MigrationElement {
  /**
   * Applies the migration element against the given connection.
   * @param conn the connection to use to execute the element.
   */
  void apply(Connection conn) throws SQLException;

  /**
   * Returns a list of the SQL statements this element has executed
   * @return
   */
  List<String> getStatements();
}
