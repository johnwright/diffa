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
import java.sql.Types
import net.lshift.diffa.kernel.config.{Domain, HibernateMigrationStep}

object Step0024 extends HibernateMigrationStep {

  def versionId = 24

  def name = "Add configuration versioning"

  def createMigration(config: Configuration) = {
    val migration = new MigrationBuilder(config)

    /**
     * Create a table that effectively is the same thing as adding an extra
     * column to the domains table, but without introducing any overhead in maintaining that table.
     */
    migration.createTable("domain_config_version").
      column("domain", Types.VARCHAR, 50, false, Domain.DEFAULT_DOMAIN.name).
      column("version", Types.INTEGER, false, 0). // Start with version 0
      pk("domain")

    migration.alterTable("domain_config_version").
      addForeignKey("fk_cfvs_dmns", "domain", "domains", "name")

    migration
  }
}
