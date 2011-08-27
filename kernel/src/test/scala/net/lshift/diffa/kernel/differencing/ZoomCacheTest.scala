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

package net.lshift.diffa.kernel.differencing

import ZoomCache._
import org.junit.Test
import net.sf.ehcache.CacheManager
import net.lshift.diffa.kernel.config.DiffaPairRef
import org.easymock.EasyMock._


class ZoomCacheTest {

  val cacheManager = new CacheManager()
  val pair = DiffaPairRef("pair", "domain")
  val diffStore = createStrictMock("diffStore", classOf[DomainDifferenceStore])
  val zoomCache = new ZoomCacheProvider(pair, diffStore, cacheManager)

  @Test(expected = classOf[InvalidZoomLevelException])
  def shouldRejectZoomLevelThatIsTooFine { zoomCache.retrieveTilesForZoomLevel(QUARTER_HOURLY + 1) }

  @Test(expected = classOf[InvalidZoomLevelException])
  def shouldRejectZoomLevelThatIsTooCoarse { zoomCache.retrieveTilesForZoomLevel(DAILY - 1) }


}