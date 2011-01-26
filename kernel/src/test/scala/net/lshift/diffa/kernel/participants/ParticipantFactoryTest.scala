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

package net.lshift.diffa.kernel.participants

import org.junit.Test
import org.junit.Assert._
import org.easymock.EasyMock._
import net.lshift.diffa.kernel.util.EasyMockScalaUtils._
import net.lshift.diffa.kernel.config.Endpoint

/**
 * Test cases for the participant factory.
 */
class ParticipantFactoryTest {
  private val protocol1 = createStrictMock("protocol1", classOf[ParticipantProtocolFactory])
  private val protocol2 = createStrictMock("protocol2", classOf[ParticipantProtocolFactory])

  private val factory = new ParticipantFactory()
  factory.registerFactory(protocol1)
  factory.registerFactory(protocol2)

  private val upstream1 = createStrictMock("upstream1", classOf[UpstreamParticipant])
  private val downstream1 = createStrictMock("downstream1", classOf[DownstreamParticipant])

    // TODO: Should not be hardcoding
  private val json = "application/json"

  checkOrder(protocol1, false)
  checkOrder(protocol2, false)
  expect(protocol1.supportsAddress("http://localhost", json)).andReturn(true).anyTimes
  expect(protocol1.supportsAddress(anyString, anyString)).andReturn(false).anyTimes
  expect(protocol2.supportsAddress("amqp://localhost", json)).andReturn(true).anyTimes
  expect(protocol2.supportsAddress(anyString, anyString)).andReturn(false).anyTimes

  val invalid = Endpoint("invalid", null, null, null, null, true)
  val jsonOverHttp = Endpoint("jsonOverHttp", "http://localhost", json, null, null, true)
  val jsonOverAmqp = Endpoint("jsonOverAmqp", "amqp://localhost", json, "changes-queue", json, true)

  @Test
  def shouldNotCreateUpstreamParticipantWhenNoFactoryAcceptsAddress {
    replay(protocol1, protocol2)

    expectsInvalidParticipantException {
      factory.createUpstreamParticipant(invalid)
    }
  }

  @Test
  def shouldNotCreateDownstreamParticipantWhenNoFactoryAcceptsAddress {
    replay(protocol1, protocol2)

    expectsInvalidParticipantException {
      factory.createDownstreamParticipant(invalid)
    }
  }

  @Test
  def shouldCreateUpstreamParticipantWhenFirstProtocolRespondsToAddress {

    expect(protocol1.createUpstreamParticipant(jsonOverHttp.url, jsonOverHttp.contentType)).andReturn(upstream1)
    replay(protocol1, protocol2)

    assertEquals(upstream1, factory.createUpstreamParticipant(jsonOverHttp))
    verify(protocol1, protocol2)
  }

  @Test
  def shouldCreateUpstreamParticipantWhenSecondProtocolRespondsToAddress {
    expect(protocol2.createUpstreamParticipant(jsonOverAmqp.url,jsonOverAmqp.contentType)).andReturn(upstream1)
    replay(protocol1, protocol2)

    assertEquals(upstream1, factory.createUpstreamParticipant(jsonOverAmqp))
    verify(protocol1, protocol2)
  }

  @Test
  def shouldCreateDownstreamParticipantWhenFirstProtocolRespondsToAddress {
    expect(protocol1.createDownstreamParticipant(jsonOverHttp.url, jsonOverHttp.contentType)).andReturn(downstream1)
    replay(protocol1, protocol2)

    assertEquals(downstream1, factory.createDownstreamParticipant(jsonOverHttp))
    verify(protocol1, protocol2)
  }

  @Test
  def shouldCreateDownstreamParticipantWhenSecondProtocolRespondsToAddress {
    expect(protocol2.createDownstreamParticipant(jsonOverAmqp.url, jsonOverHttp.contentType)).andReturn(downstream1)
    replay(protocol1, protocol2)

    assertEquals(downstream1, factory.createDownstreamParticipant(jsonOverAmqp))
    verify(protocol1, protocol2)
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