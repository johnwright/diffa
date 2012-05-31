package net.lshift.diffa.kernel.config.migrations

import org.hibernate.cfg.Configuration
import net.lshift.diffa.kernel.config.{MigrationUtil, HibernateMigrationStep}
import net.lshift.diffa.kernel.config.limits.ChangeEventRate
import net.lshift.hibernate.migrations.MigrationBuilder


object Step0030 extends HibernateMigrationStep {
  def versionId = 30
  def name = "Configure system-wide change event rate limit"

  def createMigration(config: Configuration) = {
    val migration = new MigrationBuilder(config)

    MigrationUtil.insertLimit(migration, ChangeEventRate)

    migration
  }
}
