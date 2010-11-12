/**
 * Copyright (C) 2010 LShift Ltd.
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
import org.apache.lucene.search.{TermRangeQuery, TermQuery, IndexSearcher}

trait AttributeIndexer {
  def query(key:String, value:String) : Seq[Indexable]
  def rangeQuery(key:String, lower:String, upper:String) : Seq[Indexable]
  def index(indexables:Seq[Indexable]) : Unit
}

case class Indexable(id:String, terms:Map[String,String])

class DefaultAttributeIndexer(index:Directory) extends AttributeIndexer
  with Closeable {

  val analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT)
  val writer = new IndexWriter(index, analyzer, true, IndexWriter.MaxFieldLength.UNLIMITED)
  val maxHits = 10

  def query(key:String, value:String) = {

    val query = new TermQuery(new Term(key, value))
    val searcher = new IndexSearcher(index, false)
    val hits = searcher.search(query, maxHits)

    hits.scoreDocs.map(d => {
      val doc = searcher.doc(d.doc)
      val fields = doc.getFields
      val terms = new HashMap[String,String]
      fields.filter(_.name != "id").foreach(f => terms(f.name) = f.stringValue)
      Indexable(doc.get("id"), terms)
    })
  }

  def rangeQuery(key:String, lower:String, upper:String) = {

    val query = new TermRangeQuery(key, lower, upper, true, true)
    val searcher = new IndexSearcher(index, false)
    val hits = searcher.search(query, maxHits)

    hits.scoreDocs.map(d => {
      val doc = searcher.doc(d.doc)
      val fields = doc.getFields
      val terms = new HashMap[String,String]
      fields.filter(_.name != "id").foreach(f => terms(f.name) = f.stringValue)
      Indexable(doc.get("id"), terms)
    })
  }

  def index(indexables:Seq[Indexable]) = {
    indexables.foreach(x => {
      x.terms.foreach(t => {
        val doc = new Document()
        doc.add(new Field("id", x.id, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO))
        doc.add(new Field(t._1, t._2, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO))
        writer.addDocument(doc)
      })
    })    
    writer.commit
  }

  def close = {
    writer.close
    //searcher.close
  }
}