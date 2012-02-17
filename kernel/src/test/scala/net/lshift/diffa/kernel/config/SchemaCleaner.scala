package net.lshift.diffa.kernel.config

import org.hibernate.dialect.{MySQL5Dialect, Oracle10gDialect, Dialect}
import net.lshift.diffa.kernel.util.DatabaseEnvironment

/**
 *
 */
trait SchemaCleaner {
  def clean(sysUserEnvironment: DatabaseEnvironment, appEnvironment: DatabaseEnvironment) {}
}

object SchemaCleaner {
  def forDialect(dialect: Dialect): SchemaCleaner = {
    if (dialect.isInstanceOf[Oracle10gDialect]) OracleSchemaCleaner
    else if (dialect.isInstanceOf[MySQL5Dialect]) MySqlSchemaCleaner
    else new SchemaCleaner {}
  }
}
