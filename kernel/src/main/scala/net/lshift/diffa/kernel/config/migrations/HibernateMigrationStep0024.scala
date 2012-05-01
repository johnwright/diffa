package net.lshift.diffa.kernel.config.migrations

import net.lshift.diffa.kernel.config.HibernateMigrationStep
import org.hibernate.cfg.Configuration
import net.lshift.hibernate.migrations.MigrationBuilder
import java.sql.Types

class HibernateMigrationStep0024 extends HibernateMigrationStep {

  def versionId = 24

  def name = "Add configuration versioning"

  def createMigration(config: Configuration) = {
    val migration = new MigrationBuilder(config)

    migration.createTable("domain_config_version")

    migration.createTable("domain_config_version").
      column("domain_name", Types.VARCHAR, 50, false).
      column("version", Types.INTEGER, false).
      pk("domain_name")

    migration
  }
}
