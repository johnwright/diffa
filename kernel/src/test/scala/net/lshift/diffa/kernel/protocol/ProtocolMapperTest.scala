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

package net.lshift.diffa.kernel.protocol

import org.junit.Test
import org.junit.Assert._
import org.easymock.EasyMock._
import collection.immutable.HashSet

/**
 * Test cases for the Protocol Mapper.
 */
class ProtocolMapperTest {
  val firstHandler = createStrictMock("first", classOf[ProtocolHandler])
  val secondHandler = createStrictMock("second", classOf[ProtocolHandler])
  val thirdHandler = createStrictMock("third", classOf[ProtocolHandler])
  val groups = new java.util.HashMap[String, java.util.List[ProtocolHandler]]
  val group1 = new java.util.ArrayList[ProtocolHandler]
  val group2 = new java.util.ArrayList[ProtocolHandler]
  group1.add(firstHandler)
  group1.add(secondHandler)
  group2.add(thirdHandler)
  groups.put("group1", group1)
  groups.put("group2", group2)

  expect(firstHandler.contentType).andReturn("application/x-first").anyTimes
  expect(secondHandler.contentType).andReturn("application/x-second").anyTimes
  expect(thirdHandler.contentType).andReturn("application/x-third").anyTimes
  
  replay(firstHandler, secondHandler, thirdHandler)

  val mapper = new ProtocolMapper()
  mapper.registerHandler("group1",firstHandler)
  mapper.registerHandler("group2",secondHandler)
  mapper.registerHandler("group3",thirdHandler)

  @Test
  def shouldReturnNoneWhenGroupIsNotKnown {
    assertEquals(None, mapper.lookupHandler("unknown-group", "application/x-first"))
  }

  @Test
  def shouldReturnNoneWhenContentTypeInGroupIsNotKnown {
    assertEquals(None, mapper.lookupHandler("group1", "application/x-third"))
  }

  @Test
  def shouldReturnRightHandlerWhenKnownGroupAndContentTypeIsGiven {
    assertEquals(Some(firstHandler), mapper.lookupHandler("group1", "application/x-first"))
  }
}