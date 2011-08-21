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

package net.lshift.diffa.kernel.util

import java.sql.{SQLNonTransientConnectionException, SQLException, DriverManager}

/**
 * Helper for doing a clean shutdown of a single Derby database.
 */
object DerbyHelper {

  def shutdown(dbDir: String) {
    try {
      DriverManager.getConnection("jdbc:derby:" + dbDir + ";shutdown=true")
    } catch {
      case e: SQLException if e.getErrorCode == 50000 && e.getSQLState == "XJ015" =>
        // normal Derby shutdown exception, according to example code in Derby distribution
      case e: SQLNonTransientConnectionException =>
        // actually see this exception in practice
    }
  }

}