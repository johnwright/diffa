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

import net.lshift.diffa.schema.migrations.HibernateMigrationStep
import org.hibernate.cfg.Configuration
import net.lshift.hibernate.migrations.MigrationBuilder

object Step0039 extends HibernateMigrationStep {
  def versionId = 39
  def name = "Add the 'guest' user as a member of domain 'diffa'"

  def createMigration(config: Configuration) = {
    val migration = new MigrationBuilder(config)

    migration.sql(
      """insert into members (domain_name, user_name)
        | select d.name, u.name
        | from domains d, users u
        | where d.name = '%s'
        | and u.name = '%s'
        | group by d.name, u.name""".format(
        Step0022.defaultDomainName, Step0022.defaultUserName))

    migration
  }
}
