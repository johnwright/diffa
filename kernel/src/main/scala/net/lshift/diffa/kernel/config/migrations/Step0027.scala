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
package net.lshift.diffa.kernel.config.migrations

import org.hibernate.cfg.Configuration
import net.lshift.hibernate.migrations.MigrationBuilder
import scala.collection.JavaConversions._
import net.lshift.diffa.kernel.config.{ServiceLimit, ConfigOption}

object Step0027 {

  def versionId = 27

  def name = "Remove deprecated system config values"

  def createMigration(config: Configuration) = {
    val migration = new MigrationBuilder(config)

    migration.delete("system_config_options").where("opt_key").is(ConfigOption.eventExplanationLimitKey)
    migration.delete("system_config_options").where("opt_key").is(ConfigOption.explainFilesLimitKey)

    migration.insert("limit_definitions").values(Map(
      "name" -> ServiceLimit.DIAGNOSTIC_EVENT_BUFFER_SIZE,
      "description" -> "The number of events that the DiagnosicsManager should buffer"
    ))

    migration.insert("system_limits").values(Map(
      "name" -> ServiceLimit.DIAGNOSTIC_EVENT_BUFFER_SIZE,
      "default_limit" -> new Integer(100),
      "hard_limit" -> new Integer(100)
    ))

    migration.insert("limit_definitions").values(Map(
      "name" -> ServiceLimit.EXPLAIN_FILES,
      "description" -> "The number of explain files that should be retained for later analysis"
    ))

    migration.insert("system_limits").values(Map(
      "name" -> ServiceLimit.EXPLAIN_FILES,
      "default_limit" -> new Integer(0),
      "hard_limit" -> new Integer(5)
    ))

    migration
  }
}
