/**
 * Copyright (C) 2010-2012 LShift Ltd.
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
package net.lshift.diffa.kernel.config.migrations

import java.sql.Types
import net.lshift.hibernate.migrations.CreateTableBuilder

object CommonSteps {

  def buildPendingDiffsTable(builder:CreateTableBuilder, pkType:Int = Types.BIGINT) = {
    builder.column("oid", pkType, false).
            column("domain", Types.VARCHAR, 50, false).
            column("pair", Types.VARCHAR, 50, false).
            column("entity_id", Types.VARCHAR, 50, false).
            column("detected_at", Types.TIMESTAMP, false).
            column("last_seen", Types.TIMESTAMP, false).
            column("upstream_vsn", Types.VARCHAR, 255, true).
            column("downstream_vsn", Types.VARCHAR, 255, true).
            pk("oid").
            withNativeIdentityGenerator()
  }

  def buildDiffsTable(builder:CreateTableBuilder, pkType:Int = Types.BIGINT) = {
    builder.column("seq_id", pkType, false).
            column("domain", Types.VARCHAR, 50, false).
            column("pair", Types.VARCHAR, 50, false).
            column("entity_id", Types.VARCHAR, 255, false).
            column("is_match", Types.BIT, false).
            column("detected_at", Types.TIMESTAMP, false).
            column("last_seen", Types.TIMESTAMP, false).
            column("upstream_vsn", Types.VARCHAR, 255, true).
            column("downstream_vsn", Types.VARCHAR, 255, true).
            column("ignored", Types.BIT, false).
            pk("seq_id", "domain", "pair").
            withNativeIdentityGenerator()
  }
}
