package net.lshift.hibernate.migrations;

import org.junit.Test;

import java.sql.Connection;

import static net.lshift.hibernate.migrations.HibernateHelper.mockExecutablePreparedStatement;
import static org.easymock.EasyMock.*;

/**
 * Test for creating indexes.
 */
public class CreateIndexBuilderTest {
  @Test
  public void shouldCreateIndexForSingleColumn() throws Exception {
    MigrationBuilder mb = new MigrationBuilder(HibernateHelper.configuration());
    mb.createIndex("st_idx", "some_table", "some_col");

    Connection conn = createStrictMock(Connection.class);
    expect(conn.prepareStatement("create index st_idx on some_table(some_col)")).andReturn(mockExecutablePreparedStatement());
    replay(conn);

    mb.apply(conn);
    verify(conn);
  }

  @Test
  public void shouldCreateIndexForMultipleColumns() throws Exception {
    MigrationBuilder mb = new MigrationBuilder(HibernateHelper.configuration());
    mb.createIndex("st_idx2", "some_table", "some_col", "some_col2");

    Connection conn = createStrictMock(Connection.class);
    expect(conn.prepareStatement("create index st_idx2 on some_table(some_col,some_col2)")).andReturn(mockExecutablePreparedStatement());
    replay(conn);

    mb.apply(conn);
    verify(conn);
  }
}
