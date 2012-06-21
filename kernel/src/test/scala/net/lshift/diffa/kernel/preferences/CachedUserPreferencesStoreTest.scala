/**
 * Copyright (C) 2010-2012 LShift Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.lshift.diffa.kernel.preferences

import net.lshift.diffa.kernel.util.cache.HazelcastCacheProvider

import net.lshift.diffa.schema.jooq.DatabaseFacade
import org.junit.Test
import org.junit.Assert._
import org.easymock.EasyMock._
import org.easymock.classextension.{EasyMock => E4}
import net.lshift.diffa.kernel.config.DiffaPairRef
import org.jooq.impl.Factory
import scala.collection.JavaConversions._

class CachedUserPreferencesStoreTest {

  val cacheProvider = new HazelcastCacheProvider
  val jooq = E4.createStrictMock(classOf[DatabaseFacade])
  val preferencesStore = new JooqUserPreferencesStore(jooq, cacheProvider)

  @Test
  def shouldCacheUserVisibilityItems {

    val pairs = new java.util.ArrayList[String]()
    pairs.add("pair")

    expect(jooq.execute(anyObject[Function1[Factory, java.util.List[String]]]())).andReturn(pairs).once()

    E4.replay(jooq)

    val firstCall = preferencesStore.listFilteredItems("domain", "user", FilteredItemType.SWIM_LANE)
    assertEquals(pairs.toList, firstCall.toList)

    val secondCall = preferencesStore.listFilteredItems("domain", "user", FilteredItemType.SWIM_LANE)
    assertEquals(pairs.toList, secondCall.toList)

    E4.verify(jooq)
  }
}
