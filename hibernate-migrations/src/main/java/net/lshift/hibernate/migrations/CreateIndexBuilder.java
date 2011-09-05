package net.lshift.hibernate.migrations;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Builder for creating an index.
 */
public class CreateIndexBuilder extends SingleStatementMigrationElement {
  private final String name;
  private final String table;
  private final String[] columns;

  public CreateIndexBuilder(String name, String table, String[] columns) {
    this.name = name;
    this.table = table;
    this.columns = columns;
  }

  @Override
  protected PreparedStatement prepare(Connection conn) throws SQLException {
    StringBuilder buffer = new StringBuilder();
    for (String col : columns) {
      if (buffer.length() > 0) buffer.append(",");
      buffer.append(col);
    }

    String sql = String.format("create index %s on %s(%s)", name, table, buffer.toString());
    logStatement(sql);
    return conn.prepareStatement(sql);
  }
}
