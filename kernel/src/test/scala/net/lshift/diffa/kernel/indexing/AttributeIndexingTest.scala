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
import net.lshift.diffa.kernel.participants.ParticipantType

class AttributeIndexingTest {

  val dir = new RAMDirectory()

  @Test
  def basic = {
    val indexer = new DefaultAttributeIndexer(dir)
    val bizDate = new DateTime(1875, 7, 13, 12, 0, 0, 0)
    val toIndex = Seq(Indexable(ParticipantType.UPSTREAM, "id1", Map( "bizDate" -> bizDate.toString )))
    indexer.index(toIndex)
    val byId = indexer.query(ParticipantType.UPSTREAM, "id", "id1")
    assertEquals(1, byId.length)    
    assertEquals("id1", byId(0).id)
    assertEquals(bizDate.toString(), byId(0).terms("bizDate"))
    val byRange1 = indexer.rangeQuery(ParticipantType.UPSTREAM, "bizDate", bizDate.minusDays(1).toString(), bizDate.plusDays(1).toString())
    assertEquals(1, byRange1.length)
    val byRange2 = indexer.rangeQuery(ParticipantType.UPSTREAM, "bizDate", bizDate.plusDays(1).toString(), bizDate.plusYears(1).toString())
    assertEquals(0, byRange2.length)
    val byRange3 = indexer.rangeQuery(ParticipantType.DOWNSTREAM, "bizDate", bizDate.minusDays(1).toString(), bizDate.plusDays(1).toString())
    assertEquals(0, byRange3.length)
  }
}