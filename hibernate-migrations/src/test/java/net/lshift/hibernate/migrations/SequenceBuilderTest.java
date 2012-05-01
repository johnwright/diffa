/**
 * Copyright (C) 2012 LShift Ltd.
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

public class SequenceBuilderTest {

  @Test(expected = IllegalArgumentException.class)
  public void incrementMustBeGreatherThanZero() throws Exception {
    MigrationBuilder mb = new MigrationBuilder(HibernateHelper.configuration());
    mb.sequence("some_sequence").incrementBy(0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void startValueMustBeNonNegative() throws Exception {
    MigrationBuilder mb = new MigrationBuilder(HibernateHelper.configuration());
    mb.sequence("some_sequence").startWith(-1);
  }

  @Test
  public void shouldCreateSequence() throws Exception {
    MigrationBuilder mb = new MigrationBuilder(HibernateHelper.configuration());
    mb.sequence("some_sequence")
      .incrementBy(10)
      .startWith(1001);

    Connection conn = createStrictMock(Connection.class);
    expect(conn.prepareStatement("create sequence some_sequence increment by 10 start with 1001")).
        andReturn(mockExecutablePreparedStatement());
    replay(conn);

    mb.apply(conn);
    verify(conn);
  }
}
