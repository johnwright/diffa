/**
 * Copyright (C) 2011 LShift Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.lshift.hibernate.migrations;

import org.junit.Test;

import java.sql.Connection;
import java.sql.Types;

import static net.lshift.hibernate.migrations.HibernateHelper.mockExecutablePreparedStatement;
import static org.easymock.EasyMock.*;

/**
 * Tests for table creation.
 */
public class CreateTableBuilderTest {
  @Test
  public void shouldCreateTableWithColumnThatHasDefaultSize() throws Exception {
    MigrationBuilder mb = new MigrationBuilder(HibernateHelper.configuration());
    mb.createTable("foo").column("bar", Types.INTEGER, false).pk("bar");

    Connection conn = createStrictMock(Connection.class);
    expect(conn.prepareStatement("create table foo (bar integer not null, primary key (bar))")).
        andReturn(mockExecutablePreparedStatement());
    replay(conn);

    mb.apply(conn);
    verify(conn);
  }

  @Test
  public void shouldCreateTableWithColumnThatHasExplicitSize() throws Exception {
    MigrationBuilder mb = new MigrationBuilder(HibernateHelper.configuration());
    mb.createTable("foo").column("bar", Types.INTEGER, false).column("baz", Types.VARCHAR, 4096, true).pk("bar");

    Connection conn = createStrictMock(Connection.class);
    expect(conn.prepareStatement("create table foo (bar integer not null, baz varchar(4096), primary key (bar))")).
        andReturn(mockExecutablePreparedStatement());
    replay(conn);

    mb.apply(conn);
    verify(conn);
  }

  @Test
  public void shouldCreateTableWithCompoundPrimaryKey() throws Exception {
    MigrationBuilder mb = new MigrationBuilder(HibernateHelper.configuration());
    mb.createTable("foo").
        column("bar", Types.INTEGER, false).
        column("baz", Types.VARCHAR, 4096, true).
        pk("bar", "baz");

    Connection conn = createStrictMock(Connection.class);
    expect(conn.prepareStatement("create table foo (bar integer not null, baz varchar(4096), primary key (bar, baz))")).
        andReturn(mockExecutablePreparedStatement());
    replay(conn);

    mb.apply(conn);
    verify(conn);
  }
}
