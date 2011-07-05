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

package net.lshift.diffa.kernel.participants

import org.junit.Assert._
import org.easymock.EasyMock._
import net.lshift.diffa.kernel.util.EasyMockScalaUtils._
import net.lshift.diffa.kernel.config.Endpoint
import org.junit.{Ignore, Test}

/**
 * Test cases for the participant factory.
 */
class ParticipantFactoryTest {
  private val scanning1 = createStrictMock("scanning1", classOf[ScanningParticipantFactory])
  private val scanning2 = createStrictMock("scanning2", classOf[ScanningParticipantFactory])

  private val factory = new ParticipantFactory()
  factory.registerScanningFactory(scanning1)
  factory.registerScanningFactory(scanning2)

  private val upstream1 = createStrictMock("upstream1", classOf[ScanningParticipantRef])
  private val downstream1 = createStrictMock("downstream1", classOf[ScanningParticipantRef])

    // TODO: Should not be hardcoding
  private val json = "application/json"

  checkOrder(scanning1, false)
  checkOrder(scanning2, false)
  expect(scanning1.supportsAddress("http://localhost", json)).andReturn(true).anyTimes
  expect(scanning1.supportsAddress(anyString, anyString)).andReturn(false).anyTimes
  expect(scanning2.supportsAddress("amqp://localhost", json)).andReturn(true).anyTimes
  expect(scanning2.supportsAddress(anyString, anyString)).andReturn(false).anyTimes

  val invalid = Endpoint(name = "invalid")
  val jsonOverHttp = Endpoint(name = "jsonOverHttp", url = "http://localhost", contentType = json)
  val jsonOverAmqp = Endpoint(name = "jsonOverAmqp", url = "amqp://localhost", contentType = json, inboundUrl = "changes-queue", inboundContentType = json)

  @Test
  @Ignore("Participant changes are WIP")
  def shouldNotCreateUpstreamParticipantWhenNoFactoryAcceptsAddress {
    replay(scanning1, scanning2)

    expectsInvalidParticipantException {
      factory.createUpstreamParticipant(invalid)
    }
  }

  @Test
  @Ignore("Participant changes are WIP")
  def shouldNotCreateDownstreamParticipantWhenNoFactoryAcceptsAddress {
    replay(scanning1, scanning2)

    expectsInvalidParticipantException {
      factory.createDownstreamParticipant(invalid)
    }
  }

  @Test
  @Ignore("Participant changes are WIP")
  def shouldCreateUpstreamParticipantWhenFirstProtocolRespondsToAddress {

    expect(scanning1.createParticipantRef(jsonOverHttp.scanUrl, jsonOverHttp.contentType)).andReturn(upstream1)
    replay(scanning1, scanning2)

    assertEquals(upstream1, factory.createUpstreamParticipant(jsonOverHttp))
    verify(scanning1, scanning2)
  }

  @Test
  @Ignore("Participant changes are WIP")
  def shouldCreateUpstreamParticipantWhenSecondProtocolRespondsToAddress {
    expect(scanning2.createParticipantRef(jsonOverAmqp.url,jsonOverAmqp.contentType)).andReturn(upstream1)
    replay(scanning1, scanning2)

    assertEquals(upstream1, factory.createUpstreamParticipant(jsonOverAmqp))
    verify(scanning1, scanning2)
  }

  @Test
  @Ignore("Participant changes are WIP")
  def shouldCreateDownstreamParticipantWhenFirstProtocolRespondsToAddress {
    expect(scanning1.createParticipantRef(jsonOverHttp.url, jsonOverHttp.contentType)).andReturn(downstream1)
    replay(scanning1, scanning2)

    assertEquals(downstream1, factory.createDownstreamParticipant(jsonOverHttp))
    verify(scanning1, scanning2)
  }

  @Test
  @Ignore("Participant changes are WIP")
  def shouldCreateDownstreamParticipantWhenSecondProtocolRespondsToAddress {
    expect(scanning2.createParticipantRef(jsonOverAmqp.url, jsonOverHttp.contentType)).andReturn(downstream1)
    replay(scanning1, scanning2)

    assertEquals(downstream1, factory.createDownstreamParticipant(jsonOverAmqp))
    verify(scanning1, scanning2)
  }

  def expectsInvalidParticipantException(f: => Unit) {
    try {
      f
      fail("Should have thrown InvalidParticipantAddressException")
    } catch {
      case ipae:InvalidParticipantAddressException =>
    }
  }
}