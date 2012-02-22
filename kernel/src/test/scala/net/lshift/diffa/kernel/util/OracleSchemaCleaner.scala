package net.lshift.diffa.kernel.util

import net.lshift.diffa.kernel.util.SessionHelper.sessionFactoryToSessionHelper
import org.hibernate.jdbc.Work
import java.sql.Connection

/**
 * Implements the SchemaCleaner for Oracle databases.
 */
object OracleSchemaCleaner extends SchemaCleaner {
  override def clean(sysUserEnvironment: DatabaseEnvironment, appEnvironment: DatabaseEnvironment) {
    val schemaName = appEnvironment.username
    val password = appEnvironment.password
    val dbaConfig = sysUserEnvironment.getHibernateConfiguration
    val sessionFactory = dbaConfig.buildSessionFactory

    sessionFactory.withSession(session => {
      session.doWork(new Work {
        def execute(connection: Connection) {
          val getSessionInfo = "select sid, serial# from v$session where username = '%s'".format(schemaName.toUpperCase)
          val sessionInfoStmt = connection.prepareStatement(getSessionInfo)
          val rs = sessionInfoStmt.executeQuery
          var sessions: List[(Int, Int)] = Nil
          while (rs.next()) {
            sessions = (rs.getInt("sid"), rs.getInt("serial#")) :: sessions
          }

          sessions foreach {
            sessionInfoPair: Tuple2[Int, Int] =>
              val (sid, serialnum) = sessionInfoPair
              val disconnectUser = "alter system disconnect session '%d,%d' immediate".format(sid, serialnum)
              val disconnectStmt = connection.prepareStatement(disconnectUser)
              try {
                disconnectStmt.execute
              } catch {
                case ex =>
                  println("Failed to disconnect session [%d/%d]".format(sid, serialnum))
              }
          }
        }
      })
    })

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
