package net.lshift.diffa.kernel.config

import net.lshift.diffa.kernel.util.DatabaseEnvironment
import net.lshift.diffa.kernel.util.SessionHelper.sessionFactoryToSessionHelper

/**
 * Implements the SchemaCleaner for Oracle databases.
 */
object OracleSchemaCleaner extends SchemaCleaner {
  override def clean(sysUserEnvironment: DatabaseEnvironment, appEnvironment: DatabaseEnvironment) {
    val schemaName = appEnvironment.username
    val password = appEnvironment.password
    val configuration = sysUserEnvironment.getHibernateConfiguration
    val sessionFactory = configuration.buildSessionFactory

    val dropSchemaStatement = "drop user %s cascade".format(schemaName)
    val createSchemaStatement = "create user %s identified by %s".format(schemaName, password)
    val grantPrivilegesStatement = "grant create session, dba to %s".format(schemaName)

    sessionFactory.executeOnSession(connection => {
      val stmt = connection.createStatement
      (dropSchemaStatement :: createSchemaStatement :: grantPrivilegesStatement :: Nil) foreach {
        stmtText => {
          try {
            stmt.execute(stmtText)
          } catch {
            case ex =>
              println("Failed to execute prepared statement: %s".format(stmtText))
          }
        }
      }
      stmt.close
    })
  }
}
