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
import org.apache.lucene.index.{IndexReader, Term, IndexWriter}
import collection.mutable.{ListBuffer, HashMap}
import net.lshift.diffa.kernel.differencing.{DateAttribute, StringAttribute, IntegerAttribute, TypedAttribute, Correlation, VersionCorrelationStore}
import org.apache.lucene.document._
import org.joda.time.{DateTimeZone, DateTime}
import net.lshift.diffa.kernel.config.ConfigStore

/**
 * Implementation of the VersionCorrelationStore that utilises Lucene to store (and index) the version information
 * provided. Lucene is utilised as it provides for schema-free storage, which strongly suits the dynamic schema nature
 * of pair attributes.
 */
class LuceneVersionCorrelationStore(index:Directory, configStore:ConfigStore)
    extends VersionCorrelationStore
    with Closeable {

  private val log = LoggerFactory.getLogger(getClass)

  val schemaVersionKey = "correlationStore.schemaVersion"
  configStore.maybeConfigOption(schemaVersionKey) match {
    case None      => configStore.setConfigOption(schemaVersionKey, "0")
    case Some("0") => // We're up to date
      // When new schema versions appear, we can handle their upgrade here
  }

  val analyzer = new StandardAnalyzer(Version.LUCENE_30)
  val writer = new IndexWriter(index, analyzer, true, IndexWriter.MaxFieldLength.UNLIMITED)

  def storeUpstreamVersion(id:VersionID, attributes:scala.collection.immutable.Map[String,TypedAttribute], lastUpdated: DateTime, vsn: String) = {
    log.debug("Indexing " + id + " with attributes: " + attributes)
    doDocUpdate(id, lastUpdated, doc => {
      // Update all of the upstream attributes
      applyAttributes(doc, "up.", attributes)
      updateField(doc, stringField("uvsn", vsn))
    })
  }

  def storeDownstreamVersion(id: VersionID, attributes: scala.collection.immutable.Map[String, TypedAttribute], lastUpdated: DateTime, uvsn: String, dvsn: String) = {
    log.debug("Indexing " + id + " with attributes: " + attributes)
    doDocUpdate(id, lastUpdated, doc => {
      // Update all of the upstream attributes
      applyAttributes(doc, "down.", attributes)
      updateField(doc, stringField("duvsn", uvsn))
      updateField(doc, stringField("ddvsn", dvsn))
    })
  }

  def unmatchedVersions(pairKey:String, usConstraints:Seq[QueryConstraint], dsConstraints:Seq[QueryConstraint]) = {
    val query = queryForPair(pairKey)
    query.add(new TermQuery(new Term("isMatched", "0")), BooleanClause.Occur.MUST)

    applyConstraints(query, usConstraints, "up.")
    applyConstraints(query, dsConstraints, "down.")

    val idOnlyCollector = new DocIdOnlyCollector
    val searcher = new IndexSearcher(index, false)
    searcher.search(query, idOnlyCollector)
    idOnlyCollector.allCorrelations(searcher)
  }

  def retrieveCurrentCorrelation(id:VersionID) = {
    retrieveCurrentDoc(id) match {
      case None => None
      case Some(doc) => Some(docToCorrelation(doc))
    }
  }

  def clearUpstreamVersion(id:VersionID) = {
    doClearAttributes(id, doc => {
      if (doc.get("duvsn") == null) {
        false // Remove the document
      } else {
        // Remove all the upstream attributes. Convert to sequence as middle-step to prevent ConcurrentModificationEx
        doc.getFields.toSeq.foreach(f => {
          if (f.name.startsWith("up.")) doc.removeField(f.name)
        })
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
        // Remove all the upstream attributes. Convert to sequence as middle-step to prevent ConcurrentModificationEx
        doc.getFields.toSeq.foreach(f => {
          if (f.name.startsWith("down.")) doc.removeField(f.name)
        })
        doc.removeField("duvsn")
        doc.removeField("ddvsn")

        true  // Keep the document
      }
    })
  }

  def queryUpstreams(pairKey:String, constraints:Seq[QueryConstraint]) = {
    val query = queryForPair(pairKey)
    applyConstraints(query, constraints, "up.")
      // TODO: There doesn't seem to be a good way to filter for documents that have upstream versions. Currently,
      // we're having to do it after the documents are loaded.

    val idOnlyCollector = new DocIdOnlyCollector
    val searcher = new IndexSearcher(index, false)
    searcher.search(query, idOnlyCollector)
    idOnlyCollector.allSortedCorrelations(searcher).filter(c => c.upstreamVsn != null)
  }
  def queryDownstreams(pairKey:String, constraints:Seq[QueryConstraint]) = {
    val query = queryForPair(pairKey)
    applyConstraints(query, constraints, "down.")
      // TODO: There doesn't seem to be a good way to filter for documents that have downstream versions. Currently,
      // we're having to do it after the documents are loaded.

    val idOnlyCollector = new DocIdOnlyCollector
    val searcher = new IndexSearcher(index, false)
    searcher.search(query, idOnlyCollector)
    idOnlyCollector.allSortedCorrelations(searcher).filter(c => c.downstreamUVsn != null)
  }
  
  private def queryForId(id:VersionID) = {
    val query = new BooleanQuery
    query.add(new TermQuery(new Term("pair", id.pairKey)), BooleanClause.Occur.MUST)
    query.add(new TermQuery(new Term("id", id.id)), BooleanClause.Occur.MUST)
    query
  }

  private def queryForPair(pairKey:String):BooleanQuery = {
    val query = new BooleanQuery
    query.add(new TermQuery(new Term("pair", pairKey)), BooleanClause.Occur.MUST)
    query
  }

  private def applyConstraints(query:BooleanQuery, constraints:Seq[QueryConstraint], prefix:String) = {
    // Apply all upstream constraints
    constraints.foreach {
      case r:NoConstraint                  =>   // No constraints to add
      case u:UnboundedRangeQueryConstraint =>   // No constraints to add
      case r:RangeQueryConstraint          => {
        val tq = r match {
          case DateRangeConstraint(category, lowerDate, upperDate) => new TermRangeQuery(prefix + category, formatDate(lowerDate), formatDate(upperDate), true, true)
          case IntegerRangeConstraint(category, lowerInt, upperInt) => NumericRangeQuery.newIntRange(prefix + category, lowerInt, upperInt, true, true)
        }
        query.add(tq, BooleanClause.Occur.MUST)
      }
      case s:PrefixQueryConstraint => {
        val wq = new WildcardQuery(new Term(prefix + s.category, s.prefix + "*"))
        query.add(wq, BooleanClause.Occur.MUST)
      }
      case s:SetQueryConstraint  => {
        val setMatchQuery = new BooleanQuery
        s.values.foreach(x => setMatchQuery.add(new TermQuery(new Term(prefix + s.category, x)), BooleanClause.Occur.SHOULD))
        query.add(setMatchQuery, BooleanClause.Occur.MUST)
      }
    }
  }

  private def retrieveCurrentDoc(id:VersionID) = {
    val searcher = new IndexSearcher(index, false)
    val hits = searcher.search(queryForId(id), 1)
    if (hits.scoreDocs.size == 0) {
      None
    } else {
      Some(searcher.doc(hits.scoreDocs(0).doc))
    }
  }

  private def getCurrentOrNewDoc(id:VersionID) = {
    retrieveCurrentDoc(id) match {
      case None => {
        // Nothing in the index yet for this document
        val result = new Document
        result.add(new Field("pairWithId", id.pairKey + "." + id.id, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO))
        result.add(new Field("pair", id.pairKey, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO))
        result.add(new Field("id", id.id, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO))
        result
      }
      case Some(doc) => doc
    }
  }

  private def doDocUpdate(id:VersionID, lastUpdated:DateTime, f:Document => Unit) = {
    val doc = getCurrentOrNewDoc(id)

    f(doc)

    // Update the lastUpdated field
    val oldLastUpdate = parseDate(doc.get("lastUpdated"))
    if (oldLastUpdate == null || lastUpdated.isAfter(oldLastUpdate)) {
      updateField(doc, dateField("lastUpdated", lastUpdated, indexed = false))
    }

    // Update the matched status
    val isMatched = doc.get("uvsn") == doc.get("duvsn")
    updateField(doc, boolField("isMatched", isMatched))

    // Update the timestamp
    updateField(doc, dateField("timestamp", new DateTime, indexed = false))

    updateDocument(id, doc)
    writer.commit

    docToCorrelation(doc)
  }

  private def doClearAttributes(id:VersionID, f:Document => Boolean) = {
    retrieveCurrentDoc(id) match {
      case None => Correlation.asDeleted(id.pairKey, id.id, new DateTime)
      case Some(doc) => {
        if (f(doc)) {
          // We want to keep the document. Update match status and write it out
          updateField(doc, boolField("isMatched", false))

          updateDocument(id, doc)
          writer.commit

          docToCorrelation(doc)
        } else {
          // We'll just delete the doc if it doesn't have a upstream
          writer.deleteDocuments(queryForId(id))
          writer.commit

          Correlation.asDeleted(id.pairKey, id.id, new DateTime)
        }
      }
    }
  }

  private def updateDocument(id:VersionID, doc:Document) = {
    writer.updateDocument(new Term("pairWithId", id.pairKey + "." + id.id), doc)
  }
  private def updateField(doc:Document, field:Fieldable) = {
    doc.removeField(field.name)
    doc.add(field)
  }
  
  private def stringField(name:String, value:String, indexed:Boolean = true) =
    new Field(name, value, Field.Store.YES, indexConfig(indexed), Field.TermVector.NO)
  private def dateField(name:String, dt:DateTime, indexed:Boolean = true) =
    new Field(name, formatDate(dt), Field.Store.YES, indexConfig(indexed), Field.TermVector.NO)
  private def intField(name:String, value:Int, indexed:Boolean = true) =
    (new NumericField(name, Field.Store.YES, indexed)).setIntValue(value)
  private def boolField(name:String, value:Boolean, indexed:Boolean = true) =
    new Field(name, if (value) "1" else "0", Field.Store.YES, indexConfig(indexed), Field.TermVector.NO)
  private def indexConfig(indexed:Boolean) = if (indexed) Field.Index.NOT_ANALYZED_NO_NORMS else Field.Index.NO

  private def docToCorrelation(doc:Document) = {
    Correlation(
      pairing = doc.get("pair"), id = doc.get("id"),
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

  private def applyAttributes(doc:Document, prefix:String, attributes:Map[String, TypedAttribute]) = {
    attributes.foreach { case (k, v) => {
      val vF = v match {
        case StringAttribute(s)     => stringField(prefix + k, s)
        case DateAttribute(dt)      => dateField(prefix + k, dt)
        case IntegerAttribute(intV) => intField(prefix + k, intV)
      }
      updateField(doc, vF)
    } }
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

  private def parseDate(ds:String) = {
    if (ds != null) {
      ISODateTimeFormat.dateTimeParser.parseDateTime(ds)
    } else {
      null
    }
  }
  private def formatDate(dt:DateTime) = dt.withZone(DateTimeZone.UTC).toString()
  private def parseBool(bs:String) = bs != null && bs.equals("1")

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
        docToCorrelation(doc)
      })
    }

    def allSortedCorrelations(searcher:IndexSearcher) = allCorrelations(searcher).sortBy(c => c.id)
  }

  def close = {
    writer.close
  }

  def reset() = {
    writer.deleteAll
    writer.commit
  }
}