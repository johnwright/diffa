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

import org.hibernate.cfg.Configuration
import net.lshift.hibernate.migrations.MigrationBuilder
import net.lshift.diffa.schema.migrations.MigrationStep
import java.sql.Types

object Step0041 extends MigrationStep {

  def versionId = 41

  def name = "Split out categories for endpoints and views"

  def createMigration(config: Configuration) = {
    val migration = new MigrationBuilder(config)

    migration.createTable("unique_category_view_names").
      column("domain", Types.VARCHAR, 50, false).
      column("endpoint", Types.VARCHAR, 50, false).
      column("name", Types.VARCHAR, 50, false).
      column("view_name", Types.VARCHAR, 50, false).
      pk("domain", "endpoint", "name", "view_name")

    migration.alterTable("unique_category_view_names").
      addForeignKey("fk_ucvn_evws", Array("domain", "endpoint", "view_name"), "endpoint_views", Array("domain", "endpoint", "name"))

    migration.createTable("prefix_category_views").
      column("domain", Types.VARCHAR, 50, false).
      column("endpoint", Types.VARCHAR, 50, false).
      column("name", Types.VARCHAR, 50, false).
      column("view_name", Types.VARCHAR, 50, false).
      column("prefix_length", Types.INTEGER, true).
      column("max_length", Types.INTEGER, true).
      column("step", Types.INTEGER, true).
      pk("domain", "endpoint", "view_name", "name")

    migration.alterTable("prefix_category_views").
      addForeignKey("fk_pfcv_evws", Array("domain", "endpoint", "view_name"), "endpoint_views", Array("domain", "endpoint", "name"))

    migration.alterTable("prefix_category_views").
      addForeignKey("fk_pfcv_ucns", Array("domain", "endpoint", "name", "view_name"), "unique_category_view_names", Array("domain", "endpoint", "name", "view_name"))

    migration.createTable("set_category_views").
      column("domain", Types.VARCHAR, 50, false).
      column("endpoint", Types.VARCHAR, 50, false).
      column("name", Types.VARCHAR, 50, false).
      column("view_name", Types.VARCHAR, 50, false).
      column("value", Types.VARCHAR, 255, false).
      pk("domain", "endpoint",  "view_name", "name", "value")

    migration.alterTable("set_category_views").
      addForeignKey("fk_stcv_evws", Array("domain", "endpoint", "view_name"), "endpoint_views", Array("domain", "endpoint", "name"))

    migration.alterTable("set_category_views").
      addForeignKey("fk_stcv_ucns", Array("domain", "endpoint", "name", "view_name"), "unique_category_names", Array("domain", "endpoint", "name", "view_name"))

    migration.createTable("range_category_views").
      column("domain", Types.VARCHAR, 50, false).
      column("endpoint", Types.VARCHAR, 50, false).
      column("name", Types.VARCHAR, 50, false).
      column("view_name", Types.VARCHAR, 50, false).
      column("data_type", Types.VARCHAR, 20, false).
      column("lower_bound", Types.VARCHAR, 255, true).
      column("upper_bound", Types.VARCHAR, 255, true).
      column("max_granularity", Types.VARCHAR, 20, true).
      pk("domain", "endpoint", "name", "view_name")

    migration.alterTable("range_category_views").
      addForeignKey("fk_racv_evws", Array("domain", "endpoint", "view_name"), "endpoint_views", Array("domain", "endpoint", "name"))

    // Make sure names across all category tables are unique

    migration.alterTable("range_category_views").
      addForeignKey("fk_racv_ucns", Array("domain", "endpoint", "name", "view_name"), "unique_category_names", Array("domain", "endpoint", "name", "view_name"))


    migration
  }
}
