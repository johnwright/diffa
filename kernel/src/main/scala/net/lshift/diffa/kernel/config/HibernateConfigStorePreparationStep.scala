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
import org.slf4j.{LoggerFactory, Logger}
import net.lshift.diffa.kernel.util.SessionHelper._// for 'SessionFactory.withSession'
import org.hibernate.cfg.{Environment, Configuration}

/**
 * Preparation step to ensure that the configuration for the Hibernate Config Store is in place.
 */
class HibernateConfigStorePreparationStep
    extends HibernatePreparationStep {
  protected val log:Logger = LoggerFactory.getLogger(getClass)

  /**
   * Prior to version 2 of the database, the schema version was kept in the ConfigOptions table
   */
  val schemaVersionKey = "configStore.schemaVersion"

  // The migration steps necessary to bring a hibernate configuration up-to-date. Note that these steps should be
  // in strictly ascending order.
  val migrationSteps:Seq[HibernateMigrationStep] = Seq(
    RemoveGroupsMigrationStep,
    AddSchemaVersionMigrationStep
  )

  def containsTable(sf: SessionFactory, config: Configuration, table:String) : Boolean = {
    var hasTable:Boolean = false

    sf.withSession(s => {
      s.doWork(new Work {
        def execute(connection: Connection) = {
          val props = config.getProperties
          val dbMetadata = new DatabaseMetadata(connection, Dialect.getDialect(props))

          val defaultCatalog = props.getProperty(Environment.DEFAULT_CATALOG)
          val defaultSchema = props.getProperty(Environment.DEFAULT_SCHEMA)

          hasTable = (dbMetadata.getTableMetadata(table, defaultSchema, defaultCatalog, false) != null)
        }
      })
    })

    hasTable
  }

  def prepare(sf: SessionFactory, config: Configuration) {
    // The config_options table has been in the schema since verion 0, hence we can
    // assume that if the config_options is not present at all, then we don't have a database.
    // If this table ever were to disappear in the future, then this assumption will be invalid
    val exportSchema = !containsTable(sf, config, "config_options")

    // Establish what the current version of the schema is.
    // Note that that any version above 1 should contain a schema version table
    val version = exportSchema match {
      // If we are going to export the schema, then take the latest version
      case true  => 2
      case false => {
        containsTable(sf, config, "schema_version") match {
          // If the schema version table is present, we can assume 2+
          case true  => 2
          case false => containsTable(sf, config, "pair_group") match {
            // Pair groups were deprecated in version 1
            case true  => 0
            case false => 1
          }
        }
      }
    }

    // Check if we have a schema in place

    if (exportSchema) {
      (new SchemaExport(config)).create(false, true)
        // Since we are creating a fresh schema, we need to populate the schema version as well
        sf.withSession(s => {
          s.save(SchemaVersion(migrationSteps.last.versionId))
        })

      log.info("Applied initial database schema")
    } else {
      // Maybe upgrade the schema?
      sf.withSession(s => {

        val currentVersionId = if (version < 2) {
          version
        }
        else{
          s.createQuery("select max(version) from SchemaVersion").uniqueResult().asInstanceOf[Int]
        }

        log.info("Current database version is " + currentVersionId)


        val firstStepIdx = migrationSteps.indexWhere(step => step.versionId > currentVersionId)
        if (firstStepIdx != -1) {
          s.doWork(new Work {
            def execute(connection: Connection) {
              migrationSteps.slice(firstStepIdx, migrationSteps.length).foreach(step => {
                step.migrate(config, connection)
                log.info("Upgraded database to version " + step.versionId)
                if (step.versionId > 1) {
                  s.saveOrUpdate(SchemaVersion(step.versionId))
                  s.flush
                }
              })
            }
          })
        }
      })
    }
  }
}

abstract class HibernateMigrationStep {

  /**
   * Generates a create statement for the requested table
   */
  def generateCreateSQL(tableName:String, config: Configuration) = {
    val dialect = Dialect.getDialect(config.getProperties)
    val catalog = config.getProperty( Environment.DEFAULT_CATALOG )
    val schema = config.getProperty( Environment.DEFAULT_SCHEMA )

    val mapping = config.buildMapping()
    val mappings = config.createMappings()

    val table = mappings.getTable(schema, catalog, tableName)
    table.sqlCreateString(dialect, mapping ,catalog, schema)
  }

  /**
   * The version that this step gets the database to.
   */
  def versionId:Int

  /**
   * Requests that the step perform it's necessary migration.
   */
  def migrate(config:Configuration, connection:Connection)
}

object RemoveGroupsMigrationStep extends HibernateMigrationStep {
  def versionId = 1
  def migrate(config: Configuration, connection: Connection) {
    val dialect = Dialect.getDialect(config.getProperties)

    val stmt = connection.createStatement()
    stmt.execute("alter table pair drop column " + dialect.openQuote() + "NAME" + dialect.closeQuote())
    stmt.execute("drop table pair_group")
  }
}

object AddSchemaVersionMigrationStep extends HibernateMigrationStep {
  def versionId = 2
  def migrate(config: Configuration, connection: Connection) {
    val stmt = connection.createStatement()
    stmt.execute(generateCreateSQL("schema_version", config))
  }
}