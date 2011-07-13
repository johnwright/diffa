/**
 * Copyright (C) 2010-2011 LShift Ltd.
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

package net.lshift.diffa.kernel.indexing

import org.apache.lucene.store.Directory
import org.apache.lucene.util.Version
import java.io.Closeable
import org.apache.lucene.analysis.standard.StandardAnalyzer
import scala.collection.Map
import scala.collection.JavaConversions._
import org.apache.lucene.search._
import org.slf4j.LoggerFactory
import net.lshift.diffa.kernel.events.VersionID
import org.joda.time.format.ISODateTimeFormat
import net.lshift.diffa.kernel.participants._
import collection.mutable.{ListBuffer, HashMap, HashSet}
import net.lshift.diffa.kernel.differencing._
import org.apache.lucene.document._
import net.lshift.diffa.kernel.config.ConfigStore
import org.joda.time.{LocalDate, DateTimeZone, DateTime}
import org.apache.lucene.index.{IndexReader, Term, IndexWriter}
import net.lshift.diffa.participant.scanning._

/**
 * Implementation of the VersionCorrelationStore that utilises Lucene to store (and index) the version information
 * provided. Lucene is utilised as it provides for schema-free storage, which strongly suits the dynamic schema nature
 * of pair attributes.
 */
class LuceneVersionCorrelationStore(val pairKey: String, index:Directory, configStore:ConfigStore)
    extends VersionCorrelationStore
    with Closeable {

  import LuceneVersionCorrelationStore._

  private val log = LoggerFactory.getLogger(getClass)

  val schemaVersionKey = "correlationStore.schemaVersion"
  configStore.maybeConfigOption(schemaVersionKey) match {
    case None      => configStore.setConfigOption(schemaVersionKey, "0", isInternal = true)
    case Some("0") => // We're up to date
      // When new schema versions appear, we can handle their upgrade here
  }

  val writer = new LuceneWriter(index)

  def openWriter() = writer

  def unmatchedVersions(usConstraints:Seq[ScanConstraint], dsConstraints:Seq[ScanConstraint]) = {
    val query = new BooleanQuery
    query.add(new TermQuery(new Term("isMatched", "0")), BooleanClause.Occur.MUST)

    applyConstraints(query, usConstraints, Upstream, true)
    applyConstraints(query, dsConstraints, Downstream, true)
    
    val idOnlyCollector = new DocIdOnlyCollector
    val searcher = new IndexSearcher(index, false)
    searcher.search(query, idOnlyCollector)
    idOnlyCollector.allCorrelations(searcher)
  }

  def retrieveCurrentCorrelation(id:VersionID) = {
    retrieveCurrentDoc(index, id) match {
      case None => None
      case Some(doc) => Some(docToCorrelation(doc, id.pairKey))
    }
  }

  def queryUpstreams(constraints:Seq[ScanConstraint]) = {
    val query = new BooleanQuery
    applyConstraints(query, constraints, Upstream, false)

    val idOnlyCollector = new DocIdOnlyCollector
    val searcher = new IndexSearcher(index, false)

    searcher.search(preventEmptyQuery(query), idOnlyCollector)
    idOnlyCollector.allSortedCorrelations(searcher).filter(c => c.upstreamVsn != null)
  }
  def queryDownstreams(constraints:Seq[ScanConstraint]) = {
    val query = new BooleanQuery
    applyConstraints(query, constraints, Downstream, false)

    val idOnlyCollector = new DocIdOnlyCollector
    val searcher = new IndexSearcher(index, false)
    searcher.search(preventEmptyQuery(query), idOnlyCollector)
    idOnlyCollector.allSortedCorrelations(searcher).filter(c => c.downstreamUVsn != null)
  }

  private def applyConstraints(query:BooleanQuery, constraints:Seq[ScanConstraint], partType:StoreParticipantType, allowMissing:Boolean) = {
    val prefix = partType.prefix

    // Apply all upstream constraints to a subquery
    val partQuery = new BooleanQuery
    constraints.foreach {
      case r:RangeConstraint          => {
        val tq = r match {
          case t:TimeRangeConstraint =>
            new TermRangeQuery(prefix + t.getAttributeName, formatDateTime(t.getStart), formatDateTime(t.getEnd), true, true)
          case d:DateRangeConstraint =>
            new TermRangeQuery(prefix + d.getAttributeName, formatDate(d.getStart), formatDate(d.getEnd), true, true)
          case i:IntegerRangeConstraint =>
            NumericRangeQuery.newIntRange(prefix + i.getAttributeName, i.getStart, i.getEnd, true, true)
        }
        partQuery.add(tq, BooleanClause.Occur.MUST)
      }
      case s:StringPrefixConstraint => {
        val wq = new WildcardQuery(new Term(prefix + s.getAttributeName, s.getPrefix + "*"))
        partQuery.add(wq, BooleanClause.Occur.MUST)
      }
      case s:SetConstraint  => {
        val setMatchQuery = new BooleanQuery
        s.getValues.foreach(x => setMatchQuery.add(new TermQuery(new Term(prefix + s.getAttributeName, x)), BooleanClause.Occur.SHOULD))
        partQuery.add(setMatchQuery, BooleanClause.Occur.MUST)
      }
    }

    // We don't want to add our sub-query unless it has terms, since an empty MUST matches nothing.
    if (partQuery.clauses().length > 0) {
      if (allowMissing) {
        // Since we allow the participant values to be missing (ie, only the other participant has a value for this id),
        // then we need to insert an OR query where either the value is missing or it matches the constraints.

        val missingOrMatchingQuery = new BooleanQuery
        missingOrMatchingQuery.add(new TermQuery(new Term(partType.presenceIndicator, "0")), BooleanClause.Occur.SHOULD)
        missingOrMatchingQuery.add(partQuery, BooleanClause.Occur.SHOULD)

        query.add(missingOrMatchingQuery, BooleanClause.Occur.MUST)
      } else {
        // If we don't allow this part to be missing, then enforce that it's requirement is present and correct.
        partQuery.add(new TermQuery(new Term(partType.presenceIndicator, "1")), BooleanClause.Occur.MUST)
        query.add(partQuery, BooleanClause.Occur.MUST)
      }
    }
  }

  private def preventEmptyQuery(query: BooleanQuery): Query =
    if (query.getClauses.length == 0)
      new MatchAllDocsQuery()
    else
      query

  private class DocIdOnlyCollector extends Collector {
    val docIds = new ListBuffer[Int]
    private var docBase:Int = 0

    def acceptsDocsOutOfOrder = true
    def setNextReader(reader: IndexReader, docBase: Int) = this.docBase = docBase
    def collect(doc: Int) = docIds.add(docBase + doc)
    def setScorer(scorer: Scorer) = {}   // Not needed

    def allCorrelations(searcher:IndexSearcher) = {
      docIds.map(id => {
        val doc = searcher.doc(id)
        docToCorrelation(doc, pairKey)
      })
    }

    def allSortedCorrelations(searcher:IndexSearcher) = allCorrelations(searcher).sortBy(c => c.id)
  }

  def close = {
    writer.close
  }

  def reset() = {
    writer.reset
  }
}

object LuceneVersionCorrelationStore {
  trait StoreParticipantType {
    def prefix:String
    def presenceIndicator:String
  }
  case object Upstream extends StoreParticipantType {
    val prefix = "up."
    val presenceIndicator = "hasUpstream"
  }
  case object Downstream extends StoreParticipantType {
    val prefix = "down."
    val presenceIndicator = "hasDownstream"
  }

  def retrieveCurrentDoc(index: Directory, id:VersionID) = {
    val searcher = new IndexSearcher(index, false)
    val hits = searcher.search(queryForId(id), 1)
    if (hits.scoreDocs.size == 0) {
      None
    } else {
      Some(searcher.doc(hits.scoreDocs(0).doc))
    }
  }

  def docToCorrelation(doc:Document, pairKey: String) = {
    Correlation(
      pairing = pairKey, id = doc.get("id"),
      upstreamAttributes = findAttributes(doc, "up."),
      downstreamAttributes = findAttributes(doc, "down."),
      lastUpdate = parseDate(doc.get("lastUpdated")),
      timestamp = parseDate(doc.get("timestamp")),
      upstreamVsn = doc.get("uvsn"),
      downstreamUVsn = doc.get("duvsn"),
      downstreamDVsn = doc.get("ddvsn"),
      isMatched = parseBool(doc.get("isMatched"))
    )
  }

  private def findAttributes(doc:Document, prefix:String) = {
    val attrs = new HashMap[String, String]
    doc.getFields.foreach(f => {
      if (f.name.startsWith(prefix)) {
        attrs(f.name.substring(prefix.size)) = f.stringValue
      }
    })
    attrs
  }

  def queryForId(id:VersionID) = {
    val query = new BooleanQuery
    query.add(new TermQuery(new Term("id", id.id)), BooleanClause.Occur.MUST)
    query
  }

  private def parseBool(bs:String) = bs != null && bs.equals("1")

  def parseDate(ds:String) = {
    if (ds != null) {
      ISODateTimeFormat.dateTimeParser.parseDateTime(ds)
    } else {
      null
    }
  }

  def formatDateTime(dt:DateTime) = dt.withZone(DateTimeZone.UTC).toString()
  def formatDate(dt:LocalDate) = dt.toString
}

class LuceneWriter(index: Directory) extends ExtendedVersionCorrelationWriter {
  import LuceneVersionCorrelationStore._

  private val log = LoggerFactory.getLogger(getClass)

  private val maxBufferSize = 10000

  private val updatedDocs = HashMap[VersionID, Document]()
  private val deletedDocs = HashSet[VersionID]()
  private var writer = createIndexWriter

  def storeUpstreamVersion(id:VersionID, attributes:scala.collection.immutable.Map[String,TypedAttribute], lastUpdated: DateTime, vsn: String) = {
    log.trace("Indexing upstream " + id + " with attributes: " + attributes)
    doDocUpdate(id, lastUpdated, doc => {
      // Update all of the upstream attributes
      applyAttributes(doc, "up.", attributes)
      updateField(doc, boolField(Upstream.presenceIndicator, true))
      updateField(doc, stringField("uvsn", vsn))
    })
  }

  def storeDownstreamVersion(id: VersionID, attributes: scala.collection.immutable.Map[String, TypedAttribute], lastUpdated: DateTime, uvsn: String, dvsn: String) = {
    log.trace("Indexing downstream " + id + " with attributes: " + attributes)
    doDocUpdate(id, lastUpdated, doc => {
      // Update all of the upstream attributes
      applyAttributes(doc, "down.", attributes)
      updateField(doc, boolField(Downstream.presenceIndicator, true))
      updateField(doc, stringField("duvsn", uvsn))
      updateField(doc, stringField("ddvsn", dvsn))
    })
  }

  def clearUpstreamVersion(id:VersionID) = {
    doClearAttributes(id, doc => {
      if (doc.get("duvsn") == null) {
        false // Remove the document
      } else {
        // Remove all the upstream attributes. Convert to list as middle-step to prevent ConcurrentModificationEx - see #177
        doc.getFields.toList.foreach(f => {
          if (f.name.startsWith("up.")) doc.removeField(f.name)
        })
        updateField(doc, boolField(Upstream.presenceIndicator, false))
        doc.removeField("uvsn")

        true  // Keep the document
      }
    })
  }

  def clearDownstreamVersion(id:VersionID) = {
    doClearAttributes(id, doc => {
      if (doc.get("uvsn") == null) {
        false // Remove the document
      } else {
        // Remove all the upstream attributes. Convert to list as middle-step to prevent ConcurrentModificationEx - see #177
        doc.getFields.toList.foreach(f => {
          if (f.name.startsWith("down.")) doc.removeField(f.name)
        })
        updateField(doc, boolField(Downstream.presenceIndicator, false))
        doc.removeField("duvsn")
        doc.removeField("ddvsn")

        true  // Keep the document
      }
    })
  }

  def isDirty = bufferSize > 0

  def rollback() = {
    writer.rollback()
    writer = createIndexWriter    // We need to create a new writer, since rollback will have closed the previous one
    log.info("Writer rolled back")
  }

  def flush() {
    if (isDirty) {
      updatedDocs.foreach { case (id, doc) =>
        writer.updateDocument(new Term("id", id.id), doc)
      }
      deletedDocs.foreach { id =>
        writer.deleteDocuments(queryForId(id))
      }
      writer.commit()
      updatedDocs.clear()
      deletedDocs.clear()
      log.trace("Writer flushed")
    }
  }

  def reset() {
    writer.deleteAll()
    writer.commit()
  }

  def close() {
    writer.close()
  }

  private def createIndexWriter() =
      new IndexWriter(index, new StandardAnalyzer(Version.LUCENE_30), IndexWriter.MaxFieldLength.UNLIMITED)  

  private def bufferSize = updatedDocs.size + deletedDocs.size

  private def prepareUpdate(id: VersionID, doc: Document) = {
    if (deletedDocs.remove(id)) {
      log.warn("Detected update of a document that was deleted in the same writer: " + id)
    }
    updatedDocs.put(id, doc)
    if (bufferSize >= maxBufferSize) {
      flush()
    }
  }

  private def prepareDelete(id: VersionID) = {
    if (updatedDocs.remove(id).isDefined) {
      log.warn("Detected delete of a document that was updated in the same writer: " + id)
    }
    deletedDocs.add(id)
    if (bufferSize >= maxBufferSize) {
      flush()
    }
  }

  private def getCurrentOrNewDoc(id:VersionID) = {
    if (updatedDocs.contains(id)) {
      updatedDocs(id)
    } else {
      val doc =
        if (deletedDocs.contains(id))
          None
        else
          retrieveCurrentDoc(index, id)

      doc match {
        case None => {
          // Nothing in the index yet for this document, or it's pending deletion
          val doc = new Document
          doc.add(new Field("id", id.id, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO))
          doc.add(new Field(Upstream.presenceIndicator, "0", Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO))
          doc.add(new Field(Downstream.presenceIndicator, "0", Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO))
          doc
        }
        case Some(doc) => doc
      }
    }
  }

  private def doDocUpdate(id:VersionID, lastUpdated:DateTime, f:Document => Unit) = {
    val doc = getCurrentOrNewDoc(id)

    f(doc)

    // Update the lastUpdated field
    // TODO Should it possible to allow the last updated field to be null?
    if (null != lastUpdated) {
      val oldLastUpdate = parseDate(doc.get("lastUpdated"))
      if (oldLastUpdate == null || lastUpdated.isAfter(oldLastUpdate)) {
        updateField(doc, dateTimeField("lastUpdated", lastUpdated, indexed = false))
      }
    }

    // Update the matched status
    val isMatched = doc.get("uvsn") == doc.get("duvsn")
    updateField(doc, boolField("isMatched", isMatched))

    // Update the timestamp
    updateField(doc, dateTimeField("timestamp", new DateTime, indexed = false))

    prepareUpdate(id, doc)

    docToCorrelation(doc, id.pairKey)
  }

  private def doClearAttributes(id:VersionID, f:Document => Boolean) = {
    val currentDoc =
      if (updatedDocs.contains(id))
        Some(updatedDocs(id))
      else if (deletedDocs.contains(id))
        None
      else
        retrieveCurrentDoc(index, id)

    currentDoc match {
      case None => Correlation.asDeleted(id.pairKey, id.id, new DateTime)
      case Some(doc) => {
        if (f(doc)) {
          // We want to keep the document. Update match status and write it out
          updateField(doc, boolField("isMatched", false))

          prepareUpdate(id, doc)

          docToCorrelation(doc, id.pairKey)
        } else {
          // We'll just delete the doc if it doesn't have an upstream
          prepareDelete(id)

          Correlation.asDeleted(id.pairKey, id.id, new DateTime)
        }
      }
    }
  }

  private def applyAttributes(doc:Document, prefix:String, attributes:Map[String, TypedAttribute]) = {
    attributes.foreach { case (k, v) => {
      val vF = v match {
        case StringAttribute(s)     => stringField(prefix + k, s)
        case DateAttribute(dt)      => dateField(prefix + k, dt)
        case DateTimeAttribute(dt)  => dateTimeField(prefix + k, dt)
        case IntegerAttribute(intV) => intField(prefix + k, intV)
      }
      updateField(doc, vF)
    } }
  }

  private def updateField(doc:Document, field:Fieldable) = {
    doc.removeField(field.name)
    doc.add(field)
  }

  private def stringField(name:String, value:String, indexed:Boolean = true) =
    new Field(name, value, Field.Store.YES, indexConfig(indexed), Field.TermVector.NO)
  private def dateTimeField(name:String, dt:DateTime, indexed:Boolean = true) =
    new Field(name, formatDateTime(dt), Field.Store.YES, indexConfig(indexed), Field.TermVector.NO)
  private def dateField(name:String, dt:LocalDate, indexed:Boolean = true) =
    new Field(name, formatDate(dt), Field.Store.YES, indexConfig(indexed), Field.TermVector.NO)
  private def intField(name:String, value:Int, indexed:Boolean = true) =
    (new NumericField(name, Field.Store.YES, indexed)).setIntValue(value)
  private def boolField(name:String, value:Boolean, indexed:Boolean = true) =
    new Field(name, if (value) "1" else "0", Field.Store.YES, indexConfig(indexed), Field.TermVector.NO)
  private def indexConfig(indexed:Boolean) = if (indexed) Field.Index.NOT_ANALYZED_NO_NORMS else Field.Index.NO

}