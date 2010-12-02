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
package net.lshift.diffa.kernel.frontend

import org.junit.Test
import org.junit.Assert._
import WireEvent._
import EventRegistry._
import org.joda.time.DateTime
import net.lshift.diffa.kernel.events.{DownstreamCorrelatedChangeEvent, DownstreamChangeEvent, UpstreamChangeEvent}

class WireEventTest {

  @Test
  def upstreamToWireAndBack = {
    val event = UpstreamChangeEvent("endpoint", "id", List("a","b", "c"), new DateTime, "version")
    val wire = toWire(event)
    val resolved = resolveEvent(wire)
    assertEquals(event, resolved)
  }

  @Test
  def downstreamToWireAndBack = {
    val event = DownstreamChangeEvent("endpoint", "id", List("a","b", "c"), new DateTime, "version")
    val wire = toWire(event)
    val resolved = resolveEvent(wire)
    assertEquals(event, resolved)
  }

  @Test
  def downstreamCorrelatedToWireAndBack = {
    val event = DownstreamCorrelatedChangeEvent("endpoint", "id", List("a","b", "c"), new DateTime, "up", "down")
    val wire = toWire(event)
    val resolved = resolveEvent(wire)
    assertEquals(event, resolved)
  }
}