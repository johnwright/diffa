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

package net.lshift.diffa.kernel.config

import org.hibernate.SessionFactory
import org.hibernate.jdbc.Work
import java.sql.Connection
import org.hibernate.tool.hbm2ddl.{SchemaExport, DatabaseMetadata}
import org.hibernate.dialect.Dialect
import org.hibernate.cfg.{Environment, Configuration}
import org.slf4j.{LoggerFactory, Logger}
import net.lshift.diffa.kernel.util.SessionHelper._ // for 'SessionFactory.withSession'

/**
 * Preparation step to ensure that the configuration for the Hibernate Config Store is in place.
 */
class HibernateConfigStorePreparationStep
    extends HibernatePreparationStep {
  protected val log:Logger = LoggerFactory.getLogger(getClass)

  val schemaVersionKey = "configStore.schemaVersion"

  def prepare(sf: SessionFactory, config: Configuration) = {
    var exportSchema = false

    // Check if we have a schema in place
    sf.withSession(s => {
      s.doWork(new Work {
        def execute(connection: Connection) = {
          val props = config.getProperties
          val dbMetadata = new DatabaseMetadata(connection, Dialect.getDialect(props))

          val defaultCatalog = props.getProperty(Environment.DEFAULT_CATALOG)
		      val defaultSchema = props.getProperty(Environment.DEFAULT_SCHEMA)

          if (dbMetadata.getTableMetadata("config_options", defaultSchema, defaultCatalog, false) == null) {
            // We need to export the schema
            exportSchema = true
          }
        }
      })
    })

    if (exportSchema) {
      (new SchemaExport(config)).create(false, true)

      // Apply the current version to the schema
      val configOpt = new ConfigOption(key = schemaVersionKey, value = "0", isInternal = true)
      sf.withSession(s => {
        s.save(configOpt)
      })
    
      log.info("Applied initial database schema")
    } else {
      // Maybe upgrade the schema?
      sf.withSession(s => {
        val currentVersion = s.load(classOf[ConfigOption], schemaVersionKey)

        // TODO: Apply necessary schema upgrades
      })
    }
  }
}