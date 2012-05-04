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

import net.lshift.diffa.kernel.config.HibernateMigrationStep
import org.hibernate.cfg.Configuration
import net.lshift.hibernate.migrations.MigrationBuilder
import java.sql.Types

object Step0025 extends HibernateMigrationStep {

  def versionId = 25

  def name = "Break out external http credentials"

  def createMigration(config: Configuration) = {
    val migration = new MigrationBuilder(config)

    migration.createTable("external_http_creds").
      column("domain", Types.VARCHAR, 50, false).
      column("key", Types.VARCHAR, 50, false).
      column("value", Types.VARCHAR, 255, false).
      column("scope", Types.VARCHAR, 20, false)

    migration.alterTable("external_http_creds").
      addForeignKey("fk_domain_http_creds", "domain", "domains", "name")

    migration
  }
}
