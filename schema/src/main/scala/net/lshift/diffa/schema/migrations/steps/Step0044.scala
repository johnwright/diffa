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

package net.lshift.diffa.schema.migrations.steps

import net.lshift.diffa.schema.migrations.MigrationStep
import org.hibernate.cfg.Configuration
import net.lshift.hibernate.migrations.MigrationBuilder
import net.lshift.diffa.schema.servicelimits.ExplainFiles

object Step0044 extends MigrationStep {

  def versionId = 44
  def name = "Remove service limit type for explain files"

  def createMigration(config: Configuration) = {
    val migration = new MigrationBuilder(config)

    migration.delete("pair_limits").where("name").is(ExplainFiles.key)
    migration.delete("domain_limits").where("name").is(ExplainFiles.key)
    migration.delete("system_limits").where("name").is(ExplainFiles.key)
    migration.delete("limit_definitions").where("name").is(ExplainFiles.key)

    migration
  }
}