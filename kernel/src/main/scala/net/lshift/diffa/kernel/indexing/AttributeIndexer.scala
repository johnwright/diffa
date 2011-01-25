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
import org.apache.lucene.index.{Term, IndexWriter}
import scala.collection.Map
import scala.collection.JavaConversions._
import collection.mutable.HashMap
import net.lshift.diffa.kernel.participants.ParticipantType
import org.apache.lucene.search._
import org.slf4j.LoggerFactory


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

class LuceneAttributeIndexer(index:Directory) extends AttributeIndexer with Closeable {

  private val log = LoggerFactory.getLogger(getClass)

  val analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT)
  val writer = new IndexWriter(index, analyzer, true, IndexWriter.MaxFieldLength.UNLIMITED)
  val maxHits = 10000

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

  def reset() = writer.deleteAll
}