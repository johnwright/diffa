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

package net.lshift.diffa.kernel.diag.cassandra

import net.lshift.diffa.kernel.config.DiffaPairRef
import net.lshift.diffa.kernel.diag.{ExplainLogEntry, ExplainLogStore}

import com.yammer.metrics.Metrics
import com.yammer.metrics.core.Timer
import java.io.StringWriter
import java.util.concurrent.TimeUnit._
import me.prettyprint.cassandra.utils.TimeUUIDUtils
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.{JsonFactory, JsonNode, JsonGenerator}
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat.basicDateTime
import scala.collection.JavaConversions._


class CassandraExplainLogStore(schema: Schema) extends ExplainLogStore {

  val explainLogInsertTimer = Metrics.newTimer(getClass, "explain-log-inserts", MILLISECONDS, SECONDS)
  val explainLogQueryTimer = Metrics.newTimer(getClass, "explain-log-queries", MILLISECONDS, SECONDS)
  val explainAttachmentInsertTimer = Metrics.newTimer(getClass, "explain-attachment-inserts", MILLISECONDS, SECONDS)
  val explainAttachmentsQueryTimer = Metrics.newTimer(getClass, "explain-attachment-queries", MILLISECONDS, SECONDS)

  def logPairExplanation(scanId: Option[Long], pair: DiffaPairRef, source: String, msg: String) {
    val entry = CassandraExplainLogEntry(source, msg, scanId)
    writeExplainLogEntry(pair, entry)
  }

  def logPairExplanationAttachment(scanId: Option[Long],
                                   pair: DiffaPairRef,
                                   source: String,
                                   tag: String,
                                   requestTimestamp: DateTime,
                                   f: JsonGenerator => Unit) {
    val writer = new StringWriter
    val json = (new JsonFactory).createJsonGenerator(writer)
    json.writeStartObject()
    scanId.foreach(id => json.writeNumberField("scanId", id))
    json.writeStringField("pair", pair.identifier)
    f(json)
    json.writeEndObject()
    json.flush()

    val aid = attachmentId(source, tag, requestTimestamp)
    val entry = CassandraExplainLogEntry(source = source,
                                       msg = "Attached object",
                                       scanId = scanId,
                                       attachmentId = Some(aid))
    writeExplainLogEntry(pair, entry)
    writeExplainLogAttachment(pair, aid, writer.toString)
  }

  def retrieveExplanations(pair: DiffaPairRef, start: DateTime, end: DateTime, count: Int): Iterable[ExplainLogEntry] = {
    val s = schema.explainLogEntriesSerializers
    val query = HFactory.createSliceQuery(schema.keyspace, s.rowKey, s.columnKey, s.value)
    query.setColumnFamily(schema.explainLogEntries.getName)
    query.setKey(rowKey(pair))
    query.setRange(dateTimeToUuid(start), dateTimeToUuid(end), false/* i.e. not reversed */, count)
    val result = query.execute()
    explainLogQueryTimer.update(result.getExecutionTimeNano, NANOSECONDS)
    val slice = result.get()
    val jsonMapper = new ObjectMapper

    for (col <- slice.getColumns)
      yield jsonMapper.readValue(col.getValue, classOf[CassandraExplainLogEntry]).toExplainLogEntry
  }

  def retrieveAttachment(pair: DiffaPairRef, attachmentId: String): Option[JsonNode] = {
    val s = schema.explainLogAttachmentsSerializers
    val query = HFactory.createColumnQuery(schema.keyspace, s.rowKey, s.columnKey, s.value)
    query.setColumnFamily(schema.explainLogAttachments.getName)
    query.setKey(rowKey(pair))
    query.setName(attachmentId)
    val result = query.execute()
    explainAttachmentsQueryTimer.update(result.getExecutionTimeNano, NANOSECONDS)
    Option(result.get()).map { column =>
      (new ObjectMapper).readTree(column.getValue)
    }
  }

  private def writeExplainLogEntry(pair: DiffaPairRef, entry: CassandraExplainLogEntry) {
    val entryJson = (new ObjectMapper).writeValueAsString(entry)

    insert(columnFamily = schema.explainLogEntries,
           serializers = schema.explainLogEntriesSerializers,
           rowKey = rowKey(pair),
           columnKey = entry.id,
           value = entryJson,
           ttlSeconds = 60,// TODO get from service limit
           timer = explainLogInsertTimer)
  }

  private def writeExplainLogAttachment(pair: DiffaPairRef, attachmentId: String, attachmentJson: String) {
    insert(columnFamily = schema.explainLogAttachments,
           serializers = schema.explainLogAttachmentsSerializers,
           rowKey = rowKey(pair),
           columnKey = attachmentId,
           value = attachmentJson,
           ttlSeconds = 60,// TODO get from service limit
           timer = explainAttachmentInsertTimer)
  }

  private def insert[K, N, V](columnFamily: ColumnFamilyDefinition,
                              serializers: schema.Serializers[K, N, V],
                              rowKey: K,
                              columnKey: N,
                              value: V,
                              ttlSeconds: Int,
                              timer: Timer) {

    val column = HFactory.createColumn(columnKey, value, ttlSeconds, serializers.columnKey, serializers.value)

    val result = HFactory.createMutator(schema.keyspace, serializers.rowKey)
      .insert(rowKey, columnFamily.getName, column)

    timer.update(result.getExecutionTimeNano, NANOSECONDS)
  }

  private def rowKey(pair: DiffaPairRef) = "%s:%s".format(pair.domain, pair.key)

  private def attachmentId(source: String, tag: String, requestTimestamp: DateTime) =
    "%s:%s:%s".format(source, tag, basicDateTime.withZoneUTC.print(requestTimestamp))

  private def dateTimeToUuid(dateTime: DateTime) =
    TimeUUIDUtils.getTimeUUID(dateTime.getMillis)
}