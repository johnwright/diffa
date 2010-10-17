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

package net.lshift.diffa.kernel.differencing

import CorrelationActor._
import org.junit.{After, Before, Test}
import org.easymock.EasyMock
import org.easymock.EasyMock._
import org.junit.Assert._
import net.lshift.diffa.kernel.events.VersionID

class CorrelationActorTest {

  val pairKey = "A-B"

  val store = createStrictMock("versionStore", classOf[VersionCorrelationStore])

  val supervisor = new CorrelationActorSupervisor(store)

  @Before def start = supervisor.startActor(pairKey)
  @After def stop = supervisor.stopActor(pairKey)

  @Test
  def kickTyres = {
    val id = VersionID(pairKey, "foo")
    expect(store.clearDownstreamVersion(id)).andReturn(new Correlation())
    replay(store)
    var c = clearDownstreamVersion(id)
    assertNotNull(c)
    verify(store)
  }
}