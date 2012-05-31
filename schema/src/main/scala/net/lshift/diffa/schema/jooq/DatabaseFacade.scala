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
package net.lshift.diffa.schema.jooq

import org.jooq.impl.Factory
import org.jooq.SQLDialect
import org.jooq.UpdatableRecord
import javax.sql.DataSource
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.ConnectionCallback
import org.springframework.transaction.support.{TransactionCallback, TransactionTemplate}
import java.sql.Connection
import org.springframework.transaction.TransactionStatus
import org.springframework.jdbc.datasource.DataSourceTransactionManager

class DatabaseFacade(dataSource: DataSource, dialect: String) {

  val resolvedDialect = SQLDialect.valueOf(dialect)

  private val jdbcTemplate = new JdbcTemplate(dataSource)
  private val txTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource))

  def execute[T](f: Factory => T): T = {
    txTemplate.execute(new TransactionCallback[T] {
      def doInTransaction(status: TransactionStatus) = {
        jdbcTemplate.execute(new ConnectionCallback[T] {
          def doInConnection(conn: Connection) = {
            f(new Factory(conn, resolvedDialect))
          }
        })
      }
    })
  }
  
}
