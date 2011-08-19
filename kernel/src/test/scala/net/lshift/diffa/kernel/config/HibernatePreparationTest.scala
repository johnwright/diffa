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

import org.junit.runner.RunWith
import org.junit.Assert._
import net.lshift.diffa.kernel.util.SessionHelper._
import org.slf4j.LoggerFactory
import org.hibernate.dialect.{DerbyDialect, Dialect}
import org.hibernate.mapping.{ForeignKey, Column, Table, PrimaryKey}
import org.hibernate.cfg.{Configuration, Environment}
import org.hibernate.jdbc.Work
import scala.collection.JavaConversions._
import org.junit.Test
import org.hibernate.tool.hbm2ddl.{SchemaExport, DatabaseMetadata}
import java.io.{File, InputStream}
import org.apache.commons.io.{FileUtils, IOUtils}
import org.junit.experimental.theories.{DataPoints, DataPoint, Theory, Theories}
import java.sql._
import net.lshift.diffa.kernel.util.DerbyHelper

/**
 * Test cases for ensuring that preparation steps apply to database schemas at various levels, and allow us to upgrade
 * any version to any other.
 */
@RunWith(classOf[Theories])
class HibernatePreparationTest {

  val log = LoggerFactory.getLogger(getClass)

  val genericConfig = new Configuration().setProperty("hibernate.dialect", "org.hibernate.dialect.DerbyDialect")

  // The Hibernate validateSchema method won't check for too-many tables being present, presumably since this won't
  // adversely affect it's operation. Since we do care that we delete some objects, we'll have a specific ban-list of
  // objects that we don't want to be present.
  val invalidTables = Seq(
    "pair_group"      // Removed as of the v1 migration
  )
  val invalidColumns = Map(
    "pair" -> Seq(
      "name"    // Removed as of the v1 migration
    ),
    "config_options" -> Seq(
      "is_internal"    // Removed as of the v3 migration
    )
  )

  @Theory
  def shouldBeAbleToPrepareDatabaseVersion(startVersion:StartingDatabaseVersion) {

    log.info("Testing DB version: " + startVersion.startName)

    val prepResource = this.getClass.getResourceAsStream(startVersion.startName + "-config-db.sql")
    assertNotNull(prepResource)
    val prepStmts = loadStatements(prepResource)

    val dbDir = "target/configStore-" + startVersion.startName
    FileUtils.deleteDirectory(new File(dbDir))

    val config = new Configuration().
        addResource("net/lshift/diffa/kernel/config/Config.hbm.xml").
        addResource("net/lshift/diffa/kernel/differencing/DifferenceEvents.hbm.xml").
        setProperty("hibernate.dialect", "org.hibernate.dialect.DerbyDialect").
        setProperty("hibernate.connection.url", "jdbc:derby:target/configStore-" + startVersion.startName + ";create=true").
        setProperty("hibernate.connection.driver_class", "org.apache.derby.jdbc.EmbeddedDriver").
        setProperty("hibernate.cache.region.factory_class", "net.sf.ehcache.hibernate.EhCacheRegionFactory")
    val sf = config.buildSessionFactory
    val dialect = Dialect.getDialect(config.getProperties)

    // Prepare the starting database
    sf.withSession(s => {
      s.doWork(new Work() {
        def execute(connection: Connection) {
          val stmt = connection.createStatement()
          prepStmts.foreach(prepStmt => {
            try {
              stmt.execute(prepStmt)
            } catch {
              case ex =>
                println("Failed to execute prep stmt: '" + prepStmt + "'")
                throw ex      // Higher level code will log the exception
            }
          })
          stmt.close()
        }
      })
    })

    // Upgrade the database, gather the metadata and validate the schema
    (new HibernateConfigStorePreparationStep).prepare(sf, config)
    var dbMetadata:DatabaseMetadata = null
    sf.withSession(s => {
      s.doWork(new Work() {
        def execute(connection: Connection) {
          // Load the metadata at completion
          dbMetadata = new DatabaseMetadata(connection, dialect)
        }
      })
    })
    config.validateSchema(dialect, dbMetadata)
    validateNotTooManyObjects(config, dbMetadata)

    // Ensure we can run the upgrade again cleanly
    (new HibernateConfigStorePreparationStep).prepare(sf, config)

    // Shut down the database
    DerbyHelper.shutdown(dbDir)
  }

  /**
   * Dummy test that exports the current schema to a file called 'current-schema.sql' so it can be turned into a test
   * case. This test can be enabled to run by adding the arguments '-DdoCurrentSchemaExport=1 -DforkMode=never' to
   * the maven test command.
   */
  @Test
  def exportSchema() {
    if(System.getProperty("doCurrentSchemaExport") != null) {
      val config = new Configuration().
          addResource("net/lshift/diffa/kernel/config/Config.hbm.xml").
          addResource("net/lshift/diffa/kernel/differencing/DifferenceEvents.hbm.xml").
          setProperty("hibernate.dialect", "org.hibernate.dialect.DerbyDialect").
          setProperty("hibernate.connection.url", "jdbc:derby:target/configStore-export;create=true").
          setProperty("hibernate.connection.driver_class", "org.apache.derby.jdbc.EmbeddedDriver")

      val exporter = new SchemaExport(config)
      exporter.setOutputFile("target/current-schema.sql")
      exporter.setDelimiter(";")
      exporter.execute(false, false, false, true);
    }
  }

  /**
   * Loads statements from a resource. The load process consists of reading lines, removing those starting with a
   * comment, re-joining and splitting based on ;.
   */
  private def loadStatements(r:InputStream):Seq[String] = {
    val lines = IOUtils.readLines(r).asInstanceOf[java.util.List[String]]
    val nonCommentLines = lines.filter(l => !l.trim().startsWith("--")).toSeq

    nonCommentLines.fold("")(_ + _).split(";").filter(l => l.trim().length > 0)
  }

  /**
   * Since hibernate only validates for presence, check for things we know should be gone
   */
  private def validateNotTooManyObjects(config:Configuration, dbMetadata:DatabaseMetadata) {
    val defaultCatalog = config.getProperties.getProperty(Environment.DEFAULT_CATALOG)
    val defaultSchema = config.getProperties.getProperty(Environment.DEFAULT_SCHEMA)

    invalidTables.foreach(invalidTable =>
      assertNull("Table '" + invalidTable + "' should not be present in the database",
        dbMetadata.getTableMetadata(invalidTable, defaultSchema, defaultCatalog, false)))
    invalidColumns.keys.foreach(owningTable =>
      invalidColumns(owningTable).foreach(col =>
        assertNull("Column '" + col + "' should not be present in table '" + owningTable + "'",
        dbMetadata.getTableMetadata(owningTable, defaultSchema, defaultCatalog, false).getColumnMetadata(col))))
  }
}
object HibernatePreparationTest {
  @DataPoint def emptyDb = StartingDatabaseVersion("empty")
  @DataPoint def v0 = StartingDatabaseVersion("v0")
  @DataPoint def v1 = StartingDatabaseVersion("v1")
  @DataPoint def v2 = StartingDatabaseVersion("v2")
  @DataPoint def v3 = StartingDatabaseVersion("v3")
  @DataPoint def v4 = StartingDatabaseVersion("v4")
  @DataPoint def v5 = StartingDatabaseVersion("v5")
}

case class StartingDatabaseVersion(startName:String)