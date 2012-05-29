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
package net.lshift.diffa.schema.migrations

import scala.collection.JavaConversions._
import net.lshift.hibernate.migrations.MigrationBuilder
import org.apache.commons.lang.time.DateUtils.{MILLIS_PER_SECOND, MILLIS_PER_MINUTE}

object MigrationUtil {

  /**
   * Inserts the limit definition into the DB migration
   */
  def insertLimit(migration:MigrationBuilder,
                  key: String,
                  description: String,
                  defaultLimit: java.lang.Integer,
                  hardLimit: java.lang.Integer) = {

    migration.insert("limit_definitions").values(Map(
      "name" -> key,
      "description" -> description
    ))

    migration.insert("system_limits").values(Map(
      "name" -> key,
      "default_limit" -> defaultLimit,
      "hard_limit" -> hardLimit
    ))
  }

  /// Return type is java.lang.Integer since InsertBuilder.values requires a map of AnyRef
  def minutesToMs(minutes: Int): java.lang.Integer = minutes * MILLIS_PER_MINUTE.toInt
  def secondsToMs(seconds: Int): java.lang.Integer = seconds * MILLIS_PER_SECOND.toInt
}
