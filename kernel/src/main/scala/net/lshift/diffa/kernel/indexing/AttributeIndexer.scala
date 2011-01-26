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
import org.apache.lucene.document.{Field, Document}
import org.apache.lucene.analysis.standard.StandardAnalyzer
import scala.collection.Map
import scala.collection.JavaConversions._
import org.apache.lucene.search._
import org.slf4j.LoggerFactory
import net.lshift.diffa.kernel.events.VersionID
import org.joda.time.DateTime
import net.lshift.diffa.kernel.differencing.{Correlation, VersionCorrelationStore}
import org.joda.time.format.ISODateTimeFormat
import net.lshift.diffa.kernel.participants._
import org.apache.lucene.index.{IndexReader, Term, IndexWriter}
import collection.mutable.{ListBuffer, HashMap}

/**
 * Provides basic facilities for indexing arbitrary terms so that entity attributes can be indexed in a
 * schema-less fashion. Each query result correlates back to the id of a particular entity version.
 */
trait AttributeIndexer {

  /**
   * Returns the ids of the {key,value} pairings for either the upstream or downstream participant.
   * This is a point query - it will only return terms that match exactly on the value parameter.
   */
  def query(upOrDown:ParticipantType.ParticipantType, key:String, value:String) : Seq[Indexable]

  /**
   * Returns the ids of the {key,value} pairings for either the upstream or downstream participant.
   * This is a range query - it will  return terms that lie lexiographically between the lower and
   * upper bounds.
   */
  def rangeQuery(upOrDown:ParticipantType.ParticipantType, key:String, lower:String, upper:String) : Seq[Indexable]

  /**
   * Writes the referenced terms to the index. This creates entries for the id fields as well as
   * each element in the term map.
   */
  def index(indexables:Seq[Indexable]) : Unit

  /**
   * Resets the index - use this command with caution - it will delete all of the index data,
   * which, if run inadvertly, will mean that the index will have to get rebuilt from the database.
   */
  def reset() : Unit

  /**
   * Removes all index entries that keyed on the specified Version id. This will remove entries
   * for either the upstream or downstream participants.
   */
  def deleteAttribute(upOrDown:ParticipantType.ParticipantType, id:String) : Unit
}

case class Indexable(upOrDown:ParticipantType.ParticipantType, id:String, terms:Map[String,String])

class LuceneAttributeIndexer(index:Directory)
    extends VersionCorrelationStore
    with AttributeIndexer
    with Closeable {

  private val log = LoggerFactory.getLogger(getClass)

  val analyzer = new StandardAnalyzer(Version.LUCENE_30)
  val writer = new IndexWriter(index, analyzer, true, IndexWriter.MaxFieldLength.UNLIMITED)
  val maxHits = 10000

  def storeUpstreamVersion(id:VersionID, attributes:scala.collection.immutable.Map[String,String], lastUpdated: DateTime, vsn: String) = {
    log.debug("Indexing " + id + " with attributes: " + attributes)
    doDocUpdate(id, lastUpdated, doc => {
      // Update all of the upstream attributes
      applyAttributes(doc, "up.", attributes)
      updateField(doc, "uvsn", vsn)
    })
  }

  def storeDownstreamVersion(id: VersionID, attributes: scala.collection.immutable.Map[String, String], lastUpdated: DateTime, uvsn: String, dvsn: String) = {
    log.debug("Indexing " + id + " with attributes: " + attributes)
    doDocUpdate(id, lastUpdated, doc => {
      // Update all of the upstream attributes
      applyAttributes(doc, "down.", attributes)
      updateField(doc, "duvsn", uvsn)
      updateField(doc, "ddvsn", dvsn)
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
    retrieveCurrentDoc(id) match {
      case None => Correlation.asDeleted(id.pairKey, id.id, new DateTime)
      case Some(doc) => {
        if (doc.get("duvsn") == null) {
          // We'll just delete the doc if it doesn't have a downstream
          writer.deleteDocuments(queryForId(id))
          writer.commit

          Correlation.asDeleted(id.pairKey, id.id, new DateTime)
        } else {
          // Remove all the upstream attributes. Convert to array as middle-step to prevent ConcurrentModificationEx
          doc.getFields.toSeq.foreach(f => {
            if (f.name.startsWith("up.")) doc.removeField(f.name)
          })
          doc.removeField("uvsn")
          updateField(doc, "isMatched", "0")

          updateDocument(id, doc)
          writer.commit

          docToCorrelation(doc)
        }
      }
    }
  }
  def clearDownstreamVersion(id:VersionID) = {
    retrieveCurrentDoc(id) match {
      case None => Correlation.asDeleted(id.pairKey, id.id, new DateTime)
      case Some(doc) => {
        if (doc.get("uvsn") == null) {
          // We'll just delete the doc if it doesn't have a upstream
          writer.deleteDocuments(queryForId(id))
          writer.commit

          Correlation.asDeleted(id.pairKey, id.id, new DateTime)
        } else {
          // Remove all the upstream attributes. Convert to array as middle-step to prevent ConcurrentModificationEx
          doc.getFields.toSeq.foreach(f => {
            if (f.name.startsWith("down.")) doc.removeField(f.name)
          })
          doc.removeField("dvsn")
          updateField(doc, "isMatched", "0")

          updateDocument(id, doc)
          writer.commit

          docToCorrelation(doc)
        }
      }
    }
  }
  def queryUpstreams(pairKey:String, constraints:Seq[QueryConstraint]) = {
    val query = queryForPair(pairKey)
    applyConstraints(query, constraints, "up.")
      // TODO: How do we make sure that only entries with upstreams are returned when constraints are empty?

    val idOnlyCollector = new DocIdOnlyCollector
    val searcher = new IndexSearcher(index, false)
    searcher.search(query, idOnlyCollector)
    idOnlyCollector.allCorrelations(searcher).filter(c => c.upstreamVsn != null)
  }
  def queryDownstreams(pairKey:String, constraints:Seq[QueryConstraint]) = {
    val query = queryForPair(pairKey)
    applyConstraints(query, constraints, "down.")
      // TODO: How do we make sure that only entries with downstreams are returned when constraints are empty?

    val idOnlyCollector = new DocIdOnlyCollector
    val searcher = new IndexSearcher(index, false)
    searcher.search(query, idOnlyCollector)
    idOnlyCollector.allCorrelations(searcher).filter(c => c.downstreamUVsn != null)
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
        query.add(new TermRangeQuery(prefix + r.category, r.values(0), r.values(1), true, true), BooleanClause.Occur.MUST)
      }
      case l:ListQueryConstraint  => throw new RuntimeException("ListQueryConstraint not yet implemented")
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
      updateField(doc, "lastUpdated", lastUpdated.toString, indexed = false)
    }

    // Update the matched status
    val isMatched = doc.get("uvsn") == doc.get("duvsn")
    updateField(doc, "isMatched", if (isMatched) "1" else "0")

    // Update the timestamp
    updateField(doc, "timestamp", (new DateTime).toString, indexed = false)

    updateDocument(id, doc)
    writer.commit

    docToCorrelation(doc)
  }

  private def updateDocument(id:VersionID, doc:Document) = {
    // We can't use updateDocument as it only accepts a single term for finding the old document - and our
    // ids are composite.
//    writer.deleteDocuments(queryForId(id))
    writer.updateDocument(new Term("pairWithId", id.pairKey + "." + id.id), doc)
  }
  private def updateField(doc:Document, name:String, value:String, indexed:Boolean = true) = {
    doc.removeField(name)
    doc.add(new Field(name, value, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO))
  }

  private def docToCorrelation(doc:Document) = {
    Correlation(
      pairing = doc.get("pair"), id = doc.get("id"),
      upstreamAttributes = findAttributes(doc, "up."), downstreamAttributes = findAttributes(doc, "down."),
      lastUpdate = parseDate(doc.get("lastUpdated")), timestamp = parseDate(doc.get("timestamp")),
      upstreamVsn = doc.get("uvsn"), downstreamUVsn = doc.get("duvsn"), downstreamDVsn = doc.get("ddvsn"),
      isMatched = parseBool(doc.get("isMatched"))
    )
  }

  private def applyAttributes(doc:Document, prefix:String, attributes:Map[String, String]) = {
    attributes.foreach { case (k, v) => updateField(doc, prefix + k, v) }
  }
  private def findAttributes(doc:Document, prefix:String) = {
    val attrs = new HashMap[String, String]
    doc.getFields.foreach(f => {
      if (f.name.startsWith(prefix)) attrs(f.name.substring(prefix.size)) = f.stringValue
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
  }

  /*

  def clearUpstreamVersion(id:VersionID) = {
    val timestamp = new DateTime()
    sessionFactory.withSession(s => {
      queryCurrentCorrelation(s, id) match {
        case None => {
          // Generate a new matched correlation detail
          Correlation.asDeleted(id.pairKey, id.id, timestamp)
        }
        case Some(c:Correlation) => {
          c.upstreamVsn = null

          val correlation = if (c.downstreamUVsn == null && c.downstreamDVsn == null) {
                              // No versions at all. We can remove the entity
                              s.delete(c)

                              // Generate a new matched correlation detail
                              Correlation.asDeleted(c.pairing, c.id, timestamp)
                            } else {
                              updateMatchedState(c)
                              s.save(c)
                              c
                            }
          indexer.deleteAttribute(ParticipantType.UPSTREAM,id.id)
          correlation
        }
      }
    })
  }

  def clearDownstreamVersion(id:VersionID) = {
    val timestamp = new DateTime()
    sessionFactory.withSession(s => {
      queryCurrentCorrelation(s, id) match {
        case None => {
          // Generate a new matched correlation detail
          Correlation.asDeleted(id.pairKey, id.id, timestamp)
        }
        case Some(c:Correlation) => {
          c.downstreamUVsn = null
          c.downstreamDVsn = null
          val correlation = if (c.upstreamVsn == null) {
                              // No versions at all. We can remove the entity
                              s.delete(c)

                              // Generate a new matched correlation detail
                              Correlation.asDeleted(c.pairing, c.id, timestamp)
                            } else {
                              updateMatchedState(c)
                              s.save(c)
                              c
                            }
          indexer.deleteAttribute(ParticipantType.DOWNSTREAM,id.id)
          correlation
        }
      }
    })
  }
  def queryUpstreams(pairKey:String, constraints:Seq[QueryConstraint]) = {
    sessionFactory.withSession(s => {
      val criteria = buildCriteria(s, pairKey, ParticipantType.UPSTREAM -> constraints)
      criteria.add(Restrictions.isNotNull("upstreamVsn"))
      criteria.list.map(x => x.asInstanceOf[Correlation]).toSeq
    })
  }

  def queryDownstreams(pairKey:String, constraints:Seq[QueryConstraint]) = {
    sessionFactory.withSession(s => {
      val criteria = buildCriteria(s, pairKey, ParticipantType.DOWNSTREAM -> constraints)
      criteria.add(Restrictions.or(Restrictions.isNotNull("downstreamUVsn"), Restrictions.isNotNull("downstreamDVsn")))
      criteria.list.map(x => x.asInstanceOf[Correlation]).toSeq
    })
  }

  def queryUpstreams(pairKey:String, constraints:Seq[QueryConstraint], handler:UpstreamVersionHandler) = {
    queryUpstreams(pairKey, constraints).foreach(c => {
      handler(VersionID(c.pairing, c.id), c.upstreamAttributes.toMap, c.lastUpdate, c.upstreamVsn)
    })
  }

  def queryDownstreams(pairKey:String, constraints:Seq[QueryConstraint], handler:DownstreamVersionHandler) = {
    queryDownstreams(pairKey, constraints).foreach(c => {
      handler(VersionID(c.pairing, c.id), c.downstreamAttributes.toMap, c.lastUpdate, c.downstreamUVsn, c.downstreamDVsn)
    })
  }

  private def buildCriteria(s:Session, pairKey:String, upOrDown:Tuple2[ParticipantType.ParticipantType, Seq[QueryConstraint]]*) = {
    val criteria = s.createCriteria(classOf[Correlation])
    criteria.add(Restrictions.eq("pairing", pairKey))

    upOrDown.foreach { case(partType, constraints) => {
      constraints.foreach {
        case r:NoConstraint                  =>   // No constraints to add
        case u:UnboundedRangeQueryConstraint =>   // No constraints to add
        case r:RangeQueryConstraint          => {
          val rangeIndexes = indexer.rangeQuery(partType, r.category, r.values(0), r.values(1))
          if (rangeIndexes.size == 0) {
            criteria.add(Restrictions.sqlRestriction("0 = 1"))   // Force every item to be excluded
          } else {
            criteria.add(Restrictions.in("id", rangeIndexes.map(i => i.id).toArray.asInstanceOf[Array[AnyRef]]))
          }
        }
        case l:ListQueryConstraint  => throw new RuntimeException("ListQueryConstraint not yet implemented")
      }
    }}

    criteria.addOrder(Order.asc("id"))
    criteria
  }

  private def queryCurrentCorrelation(s:Session, id:VersionID):Option[Correlation] =
    singleQueryOpt(s, "currentCorrelation", Map("key" -> id.pairKey, "id" -> id.id))

  private def updateMatchedState(c:Correlation) = {
    c.isMatched = (c.upstreamVsn == c.downstreamUVsn)
    c
  }*/

  def query(upOrDown:ParticipantType.ParticipantType, key:String, value:String)
    = executeQuery(upOrDown, new TermQuery(new Term(key, value)))
  
  def rangeQuery(upOrDown:ParticipantType.ParticipantType, key:String, lower:String, upper:String)
    = executeQuery(upOrDown, new TermRangeQuery(key, lower, upper, true, true))

  def executeQuery(upOrDown:ParticipantType.ParticipantType, query:Query) = {
    val filter = new QueryWrapperFilter(new TermQuery(new Term("type", upOrDown.toString())))
    val searcher = new IndexSearcher(index, false)
    val hits = searcher.search(query, filter, maxHits)
    log.debug("(" + hits.totalHits + ") -> [" + upOrDown + "] whilst querying: " + query )
    hits.scoreDocs.map(d => {
      val doc = searcher.doc(d.doc)
      val fields = doc.getFields
      val terms = new HashMap[String,String]
      fields.filter(_.name != "id").foreach(f => terms(f.name) = f.stringValue)
      val in = Indexable(upOrDown, doc.get("id"), terms)
      log.debug("Returning: " + in)
      in
    })
  }

  def index(indexables:Seq[Indexable]) = {
    log.debug("Indexing terms: " + indexables)
    indexables.foreach(x => {
      x.terms.foreach{ case (term,value) => {
        val doc = new Document()
        doc.add(new Field("id", x.id, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO))
        doc.add(new Field("type", x.upOrDown.toString(), Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO))
        doc.add(new Field(term, value, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO))
        writer.updateDocument(new Term("id", x.id), doc)
      }}
    })
    writer.commit
  }

  def deleteAttribute(upOrDown:ParticipantType.ParticipantType, id:String) = {
    log.debug("[ " + upOrDown + "] Deleting attribute: " + id)
    val filter = new QueryWrapperFilter(new TermQuery(new Term("type", upOrDown.toString())))
    val query = new FilteredQuery(new TermQuery(new Term("id", id)), filter)
    writer.deleteDocuments(query)
    writer.commit    
  }

  def close = {
    writer.close
  }

  def reset() = {
    writer.deleteAll
    writer.commit
  }
}