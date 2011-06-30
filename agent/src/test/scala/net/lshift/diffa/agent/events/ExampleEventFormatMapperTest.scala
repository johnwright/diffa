package net.lshift.diffa.agent.events

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

import org.junit.Assert._
import org.junit.Test
import org.apache.commons.io.IOUtils

/**
 * Unit test for example event format mapper.
 */
class ExampleEventFormatMapperTest {

  @Test
  def mapsExampleJson() {
    val example = IOUtils.toString(getClass.getResourceAsStream("/event.json"))
    val mapper = new ExampleEventFormatMapper
    val events = mapper.map(example, "exampleEndpoint")
    assertEquals(1, events.size)
    val wireEvent = events.head

    assertEquals("upstream", wireEvent.eventType)
    assertEquals("5509a836-ca75-42a4-855a-71893448cc9d", wireEvent.metadata.get("id"))
    assertEquals("exampleEndpoint", wireEvent.metadata.get("endpoint"))
    assertEquals("2011-01-24T00:00:00.000Z", wireEvent.metadata.get("lastUpdate"))
    assertEquals("479", wireEvent.metadata.get("vsn"))
    assertEquals(2, wireEvent.attributes.size)
    assertTrue(wireEvent.attributes.contains("2010-01-27T00:00:00.000Z"))
    assertTrue(wireEvent.attributes.contains("foo"))

  }
}