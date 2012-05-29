package net.lshift.diffa.schema.migrations

import org.hibernate.cfg.Configuration
import net.lshift.hibernate.migrations.MigrationBuilder

/**
 * This Step migrates a schema/database to version 25, adding default read and
 * connect timeouts for scans.
 */
object Step0025 extends HibernateMigrationStep {
  def versionId = 25

  def name = "Add default scan timeouts"

  def createMigration(config: Configuration) = {
    val migration = new MigrationBuilder(config)

    MigrationUtil.insertLimit(migration,
      key = "scan.connect.timeout",
      description = "When attempting to open a connection to scan a participant, timeout after this many milliseconds",
      defaultLimit = MigrationUtil.secondsToMs(30),
      hardLimit = MigrationUtil.minutesToMs(2))

    MigrationUtil.insertLimit(migration,
      key = "scan.read.timeout",
      description = "When reading query response data from a scan participant, timeout after not receiving data for this many milliseconds",
      defaultLimit = MigrationUtil.secondsToMs(30),
      hardLimit = MigrationUtil.minutesToMs(2))

    migration
  }
}
