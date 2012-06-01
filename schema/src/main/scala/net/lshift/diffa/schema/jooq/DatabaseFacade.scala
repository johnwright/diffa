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
import javax.sql.DataSource
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.ConnectionCallback
import org.springframework.transaction.support.{TransactionCallback, TransactionTemplate}
import org.springframework.transaction.TransactionStatus
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.jooq.conf.{RenderNameStyle, Settings}
import java.sql.{Timestamp, Connection}
import org.joda.time.format.DateTimeFormatterBuilder
import org.joda.time.DateTime

class DatabaseFacade(dataSource: DataSource, dialect: String) {

  private val resolvedDialect = SQLDialect.valueOf(dialect)
  private val settings = new Settings()
    .withRenderSchema(false)
    .withRenderNameStyle(RenderNameStyle.LOWER)

  private val jdbcTemplate = new JdbcTemplate(dataSource)
  private val txTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource))

  def execute[T](f: Factory => T): T = {
    txTemplate.execute(new TransactionCallback[T] {
      def doInTransaction(status: TransactionStatus) = {
        jdbcTemplate.execute(new ConnectionCallback[T] {
          def doInConnection(conn: Connection) = {
            f(new Factory(conn, resolvedDialect, settings))
          }
        })
      }
    })
  }
  
}

object DatabaseFacade {

  private val timestampFormatter = new DateTimeFormatterBuilder()
    .appendPattern("yyyy-MM-dd HH:mm:ss'.'").appendFractionOfSecond(0, 9).toFormatter()

  def timestampToDateTime(timestamp: Timestamp) =
    timestampFormatter.parseDateTime(timestamp.toString)

  def dateTimeToTimestamp(dateTime: DateTime) = {
    val str = timestampFormatter.print(dateTime)
    Timestamp.valueOf(if (str.endsWith(".")) str.substring(0, str.length - 1) else str)
  }
}
