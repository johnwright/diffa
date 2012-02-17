package net.lshift.diffa.kernel.config

/**
 * Resolve a Hibernate dialect (diffa.hibernate.dialect) to a name corresponding to a file suffix identifying
 * the dialect-specific statements used to initialise a database to a specific version.
 *
 * User: anthony
 * Date: 14/02/12
 * Time: 15:21
 */

object DialectLookup {
  val hibernateDialect = "org.hibernate.dialect.HSQLDialect"
  val mysqlDialect = "org.hibernate.dialect.MySQL5Dialect"
  private val hashmap = Map[String, String](
    hibernateDialect -> "",
    mysqlDialect -> "-mysql"
  )

  def supportedDialects = hibernateDialect :: mysqlDialect :: Nil

  def lookupName(dialect: String): String = hashmap.getOrElse(dialect, "")
}
