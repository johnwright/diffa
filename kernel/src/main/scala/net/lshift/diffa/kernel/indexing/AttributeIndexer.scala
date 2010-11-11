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
import org.apache.lucene.search.{IndexSearcher, TopScoreDocCollector}
import org.apache.lucene.queryParser.QueryParser
import java.io.Closeable
import org.apache.lucene.document.{Field, Document}
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.analysis.standard.StandardAnalyzer

trait AttributeIndexer {
  def query(key:String) : String
  def index(value:String) : Unit
}

class DefaultAttributeIndexer(index:Directory,field:String) extends AttributeIndexer
  with Closeable {

  val analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT)
  val writer = new IndexWriter(index, analyzer, true, IndexWriter.MaxFieldLength.UNLIMITED)
  val parser = new QueryParser(Version.LUCENE_CURRENT, field, analyzer)
  val hitsPerPage = 10

  val collector = TopScoreDocCollector.create(hitsPerPage, true)

  def query(key:String) = {
    val query = parser.parse(key)
    val searcher = new IndexSearcher(index, true)
    searcher.search(query, collector)
    val hits = collector.topDocs().scoreDocs
    val doc = searcher.doc(hits(0).doc)
    doc.get(field)
  }

  def index(value:String) = {
    val doc = new Document()
    doc.add(new Field(field, value, Field.Store.YES, Field.Index.ANALYZED))
    writer.addDocument(doc)
    writer.close
  }

  def close = {
    //writer.close
    //searcher.close
  }
}