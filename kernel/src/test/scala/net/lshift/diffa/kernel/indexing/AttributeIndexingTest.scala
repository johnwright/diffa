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

import org.junit.Test
import org.junit.Assert._
import org.apache.lucene.store.RAMDirectory
import scala.collection.Map
import org.joda.time.DateTime

class AttributeIndexingTest {

  val dir = new RAMDirectory()

  @Test
  def basic = {
    val indexer = new DefaultAttributeIndexer(dir)
    val bizDate = new DateTime(1875, 7, 13, 12, 0, 0, 0).toString()
    val toIndex = Seq(Indexable("id1", Map( "bizDate" -> bizDate )))
    indexer.index(toIndex)
    val byId = indexer.query("id", "id1")
    assertEquals(1, byId.length)    
    assertEquals("id1", byId(0).id)
    assertEquals(bizDate, byId(0).terms("bizDate"))    
  }
}