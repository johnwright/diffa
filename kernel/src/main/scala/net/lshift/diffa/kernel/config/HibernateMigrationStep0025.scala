package net.lshift.diffa.kernel.config

import org.hibernate.cfg.Configuration
import net.lshift.hibernate.migrations.MigrationBuilder

import scala.collection.JavaConversions._

/**
 * This Step migrates a schema/database to version 25, adding default read and
 * connect timeouts for scans.
 */
object HibernateMigrationStep0025 extends HibernateMigrationStep {
  def versionId = 25

  def name = "Add default scan timeouts"

  def createMigration(config: Configuration) = {
    val migration = new MigrationBuilder(config)

    val defaultTimeoutMinutes = 5
    val maxTimeoutMinutes = 30

    List(
      (ServiceLimit.SCAN_CONNECT_TIMEOUT_KEY,
        "When attempting to open a connection to scan a participant, timeout after this many milliseconds"
        ),
      (ServiceLimit.SCAN_READ_TIMEOUT_KEY,
        "When reading query response data from a scan participant, timeout after not receiving data for this many milliseconds"
        )
    ) foreach { limDef =>
      val (name, desc) = limDef
      migration.insert("limit_definitions").values(Map(
        "name" -> name,
        "description" -> desc
      ))

      migration.insert("system_limits").values(Map(
        "name" -> name,
        "default_limit" -> minutes_in_ms(defaultTimeoutMinutes),
        "hard_limit" -> minutes_in_ms(maxTimeoutMinutes)
      ))
    }

    migration
  }

  private final val SEC_PER_MIN = 60
  private final val MS_PER_S = 1000

  /// Return type is java.lang.Integer since InsertBuilder.values requires a map of AnyRef
  private def minutes_in_ms(minutes: Int): java.lang.Integer = minutes * SEC_PER_MIN * MS_PER_S
}
