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

import java.io.{PrintWriter, FileWriter, File, InputStream }
import java.sql._
import org.junit.runner.RunWith
import org.junit.Assert._
import net.lshift.diffa.kernel.util.SessionHelper._
import org.slf4j.LoggerFactory
import org.hibernate.dialect.Dialect
import org.hibernate.cfg.{Configuration, Environment}
import org.hibernate.jdbc.Work
import scala.collection.JavaConversions._
import org.hibernate.tool.hbm2ddl.{SchemaExport, DatabaseMetadata}
import org.apache.commons.io.{FileUtils, IOUtils}
import org.junit.experimental.theories.{DataPoints, DataPoint, Theory, Theories}
import org.easymock.EasyMock._
import org.hibernate.{Session, SessionFactory}
import net.lshift.diffa.kernel.util.{EasyMockScalaUtils, DatabaseEnvironment}
import org.junit.{Ignore, Test}
import org.hsqldb.Database

/**
 * Test cases for ensuring that preparation steps apply to database schemas at various levels, and allow us to upgrade
 * any version to any other.
 */

@RunWith(classOf[Theories])
class HibernatePreparationTest {

  val log = LoggerFactory.getLogger(getClass)

  // The Hibernate validateSchema method won't check for too-many tables being present, presumably since this won't
  // adversely affect it's operation. Since we do care that we delete some objects, we'll have a specific ban-list of
  // objects that we don't want to be present.
  val invalidTables = Seq(
    "pair_group"      // Removed as of the v1 migration
  )
  val invalidColumns = Map(
    "pair" -> Seq(
      "name"    // Removed as part of the v1 migration
    ),
    "config_options" -> Seq(
      "is_internal"    // Removed as part of the v3 migration
    ),
    "endpoint" -> Seq(
      "inbound_content_type",   // Removed as part of the v13 migration
      "content_type"   // Removed as of the v15 migration
    )
  )
  
  @Test
  def migrationStepsShouldBeOrderedCorrectly = {
    val steps = HibernateConfigStorePreparationStep.migrationSteps
    for (i <- 0 until steps.length) {
      val msg = "Attempting to verify version id of step [%s]".format(steps(i).name)
      assertEquals(msg, i + 1, steps(i).versionId)
    }
  }

  /**
   * From v0 (earliest version), the deployer should be able to upgrade any supported system to the latest version.
   *
   * Supported systems are identified in DialectLookup.
   * The associated DDL statements are named (differentiated by suffix) accordingly and are located at run-time.
   */
  @Test
  def shouldBeAbleToUpgradeToLatestDatabaseVersion {
    val databaseEnvironment = DatabaseEnvironment.customEnvironment
    val adminEnvironment = TestDatabaseEnvironments.adminEnvironment

    cleanSchema(adminEnvironment, databaseEnvironment)
    waitForSchemaCreation(databaseEnvironment, pollIntervalMs = 100L, timeoutMs = 10000L)
    
    val dbConfig = createSecureConfig(databaseEnvironment)
    val dialect = Dialect.getDialect(dbConfig.getProperties)
    val sessionFactory = dbConfig.buildSessionFactory

    log.info("Operating on database %s".format(databaseEnvironment.dbName))

    // Install starting version of schema
    runMigrationStep(HibernateMigrationStep0, dbConfig, sessionFactory)
    
    log.info("Installed initial schema")

    // Upgrade to latest version
    (new HibernateConfigStorePreparationStep).prepare(sessionFactory, dbConfig)

    log.info("Upgraded schema")

    // validate the correctness of the schema
    val metadata = sessionFactory.withSession(s => retrieveMetadata(s, dialect))
    assertNotSame(None, metadata)
    dbConfig.validateSchema(dialect, metadata.get)
    validateNotTooManyObjects(dbConfig, metadata.get)

    // Verify that a further attempt to upgrade to the latest version doesn't fall over (no op)
    // TODO: move this to a separate test
    (new HibernateConfigStorePreparationStep).prepare(sessionFactory, dbConfig)
  }
  
  private def runMigrationStep(step: HibernateMigrationStep, config: Configuration, sessionFactory: SessionFactory) {
    val migration = step.createMigration(config)

    sessionFactory.executeOnSession(conn => {
      try {
        migration.apply(conn)
      } catch {
        case ex =>
          println("Failed to prepare the database - attempted to execute the following statements for step " + step.versionId + ":")
          println("_" * 80)
          println()
          migration.getStatements.foreach(println(_))
          println("_" * 80)
          println()
          throw ex      // Higher level code will log the exception
      }
    })
  }

  @Theory
  def shouldBeAbleToPrepareDatabaseVersion(startVersion:StartingDatabaseVersion) {

    log.info("Testing DB version: " + startVersion.startName)

    val prepResource = this.getClass.getResourceAsStream(startVersion.startName + "-config-db.sql")
    assertNotNull(prepResource)
    val prepStmts = loadStatements(prepResource)

    val dbDir = "target/configStore-" + startVersion.startName
    FileUtils.deleteDirectory(new File(dbDir))

    val hibernateEnv = TestDatabaseEnvironments.hsqldbEnvironment("configStore-%s".format(startVersion.startName))
    val config = hibernateEnv.getHibernateConfiguration

    log.info("building session factory for configured database [%s (%s->%s)] as [%s/%s]".format(
    hibernateEnv.url, hibernateEnv.driver, hibernateEnv.dialect,
    hibernateEnv.username, hibernateEnv.password))

    val sf = config.buildSessionFactory
    val dialect = Dialect.getDialect(config.getProperties)

    // Prepare the starting database
    sf.executeStatements(prepStmts, treatErrorsAsFatal = true)

    // Upgrade the database, gather the metadata and validate the schema
    (new HibernateConfigStorePreparationStep).prepare(sf, config)
    val dbMetadata = sf.withSession(s => retrieveMetadata(s,  dialect))

    config.validateSchema(dialect, dbMetadata.get)
    validateNotTooManyObjects(config, dbMetadata.get)

    // Ensure we can run the upgrade again cleanly
    (new HibernateConfigStorePreparationStep).prepare(sf, config)

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
      setProperty("hibernate.dialect", DatabaseEnvironment.DIALECT).
      setProperty("hibernate.connection.url", DatabaseEnvironment.substitutableURL("configStore-export")).
      setProperty("hibernate.connection.driver_class", DatabaseEnvironment.DRIVER).
      setProperty("hibernate.connection.username", DatabaseEnvironment.USERNAME).
      setProperty("hibernate.connection.password", DatabaseEnvironment.PASSWORD)

      val exporter = new SchemaExport(config)
      val outputFile = "target/current-schema.sql"
      exporter.setOutputFile(outputFile)
      exporter.setDelimiter(";")
      exporter.execute(false, false, false, true);

      val prepStep = new HibernateConfigStorePreparationStep
      val freshStep = new FreshMigrationStep(prepStep.migrationSteps.last.versionId)
      val freshMigration = freshStep.createMigration(config)

      val mockConn = createStrictMock(classOf[Connection])
      val mockPreparedStatement = createNiceMock(classOf[PreparedStatement])
      expect(mockPreparedStatement.execute()).andStubReturn(true)
      expect(mockConn.prepareStatement(EasyMockScalaUtils.anyString)).andStubReturn(mockPreparedStatement)
      replay(mockConn, mockPreparedStatement);

      freshMigration.apply(mockConn)
      println(freshMigration.getStatements.toSeq)

      val outputWriter = new PrintWriter(new FileWriter(outputFile, true))
      try {
        freshMigration.getStatements.foreach(s => outputWriter.println(s))
      } finally {
        outputWriter.flush()
        outputWriter.close()
      }
    }
  }

  @Test
  def verifyExternalDatabase() {
    if(System.getProperty("verifyExternalDB") != null) {
      val config = new Configuration().
      addResource("net/lshift/diffa/kernel/config/Config.hbm.xml").
      addResource("net/lshift/diffa/kernel/differencing/DifferenceEvents.hbm.xml").
      setProperty("hibernate.dialect", DatabaseEnvironment.DIALECT).
      setProperty("hibernate.connection.url", DatabaseEnvironment.substitutableURL("configStore-export")).
      setProperty("hibernate.connection.driver_class", DatabaseEnvironment.DRIVER).
      setProperty("hibernate.connection.username", DatabaseEnvironment.USERNAME).
      setProperty("hibernate.connection.password", DatabaseEnvironment.PASSWORD).
      setProperty("hibernate.cache.region.factory_class", "net.sf.ehcache.hibernate.EhCacheRegionFactory").
      setProperty("hibernate.cache.use_second_level_cache", "true")
      val sf = config.buildSessionFactory
      (new HibernateConfigStorePreparationStep).prepare(sf, config)
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

  private def createSecureConfig(env: DatabaseEnvironment): Configuration = {
    System.setProperty("javax.net.ssl.trustStore", env.caKeystore)
    System.setProperty("javax.net.ssl.trustStoreType", "PKCS12")
    System.setProperty("javax.net.ssl.keyStore", "diffa-ks.jks")
    System.setProperty("javax.net.ssl.keyStoreType", "JKS")
    env.getHibernateConfiguration.
      setProperty("hibernate.connection.requireSSL", "true").
      setProperty("hibernate.verifyServerCertificate", "false")
  }

  /**
   * Drop and recreate the named schema/database in order to provide a clean slate to test from.
   */
  private def cleanSchema(sysenv: DatabaseEnvironment, appenv: DatabaseEnvironment) {
    val sysConfig = sysenv.getHibernateConfiguration

    val cleaner = SchemaCleaner.forDialect(Dialect.getDialect(sysConfig.getProperties))
    cleaner.clean(sysenv, appenv)
  }

  private def waitForSchemaCreation(newDbEnviron: DatabaseEnvironment, pollIntervalMs: Long, timeoutMs: Long) {
    val config = newDbEnviron.getHibernateConfiguration
    val sessionFactory = config.buildSessionFactory
    var connected = false
    var failCount = 0
    val failThreshold = timeoutMs / pollIntervalMs

    while (connected) {
      try {
        sessionFactory.openSession
        connected = true
      } catch {
        case ex =>
          Thread.sleep(pollIntervalMs)
          failCount += 1
          if (failCount >= failThreshold)
            throw ex
      }
    }

  }
  
  private def retrieveMetadata(session: Session, dialect: Dialect): Option[DatabaseMetadata] = {
    var metadata: Option[DatabaseMetadata] = None

    session.doWork(new Work() {
      def execute(connection: Connection) {
        metadata = Some(new DatabaseMetadata(connection, dialect))
      }
    })

    metadata
  }

  private def retrieveVersionStatements(versionName: String, dialectName: String): Seq[String] = {
    val stmtsResourceOrNull = getClass.getResourceAsStream("%s-config-db%s.sql".format(versionName, dialectName))
    assertNotNull(stmtsResourceOrNull)
    loadStatements(stmtsResourceOrNull)
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
  @DataPoint def v6 = StartingDatabaseVersion("v6")
  @DataPoint def v7 = StartingDatabaseVersion("v7")
  @DataPoint def v8 = StartingDatabaseVersion("v8")
  @DataPoint def v9 = StartingDatabaseVersion("v9")
  @DataPoint def v10 = StartingDatabaseVersion("v10")
  @DataPoint def v11 = StartingDatabaseVersion("v11")
  @DataPoint def v12 = StartingDatabaseVersion("v12")
  @DataPoint def v13 = StartingDatabaseVersion("v13")
  @DataPoint def v14 = StartingDatabaseVersion("v14")
  @DataPoint def v15 = StartingDatabaseVersion("v15")
  @DataPoint def v16 = StartingDatabaseVersion("v16")
  @DataPoint def v17 = StartingDatabaseVersion("v17")
  @DataPoint def v19 = StartingDatabaseVersion("v19")
}

case class StartingDatabaseVersion(startName:String)
