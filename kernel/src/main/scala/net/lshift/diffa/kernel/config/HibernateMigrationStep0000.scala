package net.lshift.diffa.kernel.config

import org.hibernate.cfg.Configuration
import net.lshift.hibernate.migrations.MigrationBuilder
import java.sql.Types
import scala.collection.JavaConversions._

/**
 * This Step 'migrates' a schema/database to version 0 -
 * that is, it creates the base schema from scratch.
 */
object HibernateMigrationStep0000 extends HibernateMigrationStep {
  def versionId = 0
  def name = "Create schema"
  def createMigration(config: Configuration) = {
    val migration = new MigrationBuilder(config)

    migration.createTable("category_descriptor").
      column("category_id", Types.INTEGER, false).
      column("constraint_type", Types.VARCHAR, 20, false).
      column("prefix_length", Types.INTEGER, true).
      column("max_length", Types.INTEGER, true).
      column("step", Types.INTEGER, true).
      pk("category_id").
      withNativeIdentityGenerator()

    migration.createTable("config_options").
      column("domain", Types.VARCHAR, 255, false).// TODO Domain.DEFAULT_DOMAIN.name
      column("opt_key", Types.VARCHAR, 255, false).
      column("opt_val", Types.VARCHAR, 255, true).
      pk("opt_key")

    migration.createTable("domains").
      column("name", Types.VARCHAR, 255, false).
      pk("name")
    migration.insert("domains").values(Map("name" -> Domain.DEFAULT_DOMAIN.name))

    migration.createTable("endpoint").
      column("domain", Types.VARCHAR, 255, false).// TODO Domain.DEFAULT_DOMAIN.name
      column("name", Types.VARCHAR, 255, false).
      column("scan_url", Types.VARCHAR, 255, true).
      column("content_retrieval_url", Types.VARCHAR, 255, true).
      column("version_generation_url", Types.VARCHAR, 255, true).
      column("inbound_url", Types.VARCHAR, 255, true).
      column("content_type", Types.VARCHAR, 255, false).
      column("inbound_content_type", Types.VARCHAR, 255, true).
      pk("name")

    migration.createTable("endpoint_categories").
      column("id", Types.VARCHAR, 255, false).
      column("category_descriptor_id", Types.INTEGER, false).
      column("name", Types.VARCHAR, 255, false).
      pk("id", "name")
    
    migration.createTable("escalations").
      column("name", Types.VARCHAR, 255, false).
      column("pair_key", Types.VARCHAR, 255, false).
      column("action", Types.VARCHAR, 255, false).
      column("action_type", Types.VARCHAR, 255, false).
      column("event", Types.VARCHAR, 255, false).
      column("origin", Types.VARCHAR, 255, false).
      pk("name", "pair_key")

    migration.createTable("members").
      column("domain_name", Types.VARCHAR, 255, false).
      column("user_name", Types.VARCHAR, 255, false).
      pk("user_name", "domain_name")
    
    migration.createTable("pair").
      column("domain", Types.VARCHAR, 255, false).// TODO Domain.DEFAULT_DOMAIN.name
      column("pair_key", Types.VARCHAR, 255, false).
      column("upstream", Types.VARCHAR, 255, false).
      column("downstream", Types.VARCHAR, 255, false).
      column("version_policy_name", Types.VARCHAR, 255, true).
      column("matching_timeout", Types.INTEGER, true).
      column("scan_cron_spec", Types.VARCHAR, 255, true).
      pk("pair_key")

    migration.createTable("prefix_category_descriptor").
      column("id", Types.INTEGER, false).
      pk("id")

    migration.createTable("range_category_descriptor").
      column("id", Types.INTEGER, false).
      column("data_type", Types.VARCHAR, 255, true).
      column("upper_bound", Types.VARCHAR, 255, true).
      column("lower_bound", Types.VARCHAR, 255, true).
      column("max_granularity", Types.VARCHAR, 255, true).
      pk("id")

    migration.createTable("repair_actions").
      column("name", Types.VARCHAR, 255, false).
      column("pair_key", Types.VARCHAR, 255, false).
      column("url", Types.VARCHAR, 255, true).
      column("scope", Types.VARCHAR, 255, true).
      pk("name", "pair_key")

    migration.createTable("schema_version").
      column("version", Types.INTEGER, false).
      pk("version")
    migration.insert("schema_version").
      values(Map("version" -> new java.lang.Integer(versionId)))

    migration.createTable("set_category_descriptor").
      column("id", Types.INTEGER, false).
      pk("id")

    migration.createTable("set_constraint_values").
      column("value_id", Types.INTEGER, false).
      column("value_name", Types.VARCHAR, 255, false).
      pk("value_id", "value_name")

    migration.createTable("system_config_options").
      column("opt_key", Types.VARCHAR, 255, false).
      column("opt_val", Types.VARCHAR, 255, false).
      pk("opt_key")

    migration.createTable("users").
      column("name", Types.VARCHAR, 255, false).
      column("email", Types.VARCHAR, 255, true).
      pk("name")

    migration.alterTable("config_options").
      addForeignKey("FK80C74EA1C3C204DC", "domain", "domains", "name")

    migration.alterTable("endpoint").
      addForeignKey("FK67C71D95C3C204DC", "domain", "domains", "name")

    migration.alterTable("endpoint_categories").
      addForeignKey("FKEE1F9F06BC780104", "id", "endpoint", "name").
      addForeignKey("FKEE1F9F06B6D4F2CB", "category_descriptor_id", "category_descriptor", "category_id")

    migration.alterTable("escalations").
      addForeignKey("FK2B3C687E7D35B6A8", "pair_key", "pair", "pair_key")

    migration.alterTable("pair").
      addForeignKey("FK3462DAC3C204DC", "domain", "domains", "name").
      addForeignKey("FK3462DA25F0B1C4", "upstream", "endpoint", "name").
      addForeignKey("FK3462DA4242E68B", "downstream", "endpoint", "name")

    migration.alterTable("members").
      addForeignKey("FK388EC9191902E93E", "domain_name", "domains", "name").
      addForeignKey("FK388EC9195A11FA9E", "user_name", "users", "name")

    migration.alterTable("prefix_category_descriptor").
      addForeignKey("FK46474423466530AE", "id", "category_descriptor", "category_id")

    migration.alterTable("range_category_descriptor").
      addForeignKey("FKDC53C74E7A220B71", "id", "category_descriptor", "category_id")

    migration.alterTable("repair_actions").
      addForeignKey("FKF6BE324B7D35B6A8", "pair_key", "pair", "pair_key")

    migration.alterTable("set_category_descriptor").
      addForeignKey("FKA51D45F39810CA56", "id", "category_descriptor", "category_id")

    migration.alterTable("set_constraint_values").
      addForeignKey("FK96C7B32744035BE4", "value_id", "category_descriptor", "category_id")

    migration.insert("config_options").
      values(Map("opt_key" -> "configStore.schemaVersion", "opt_val" -> "0", "is_internal" -> new java.lang.Integer(1)))

    migration
  }
}
