/**
 * Copyright (C) 2010-2012 LShift Ltd.
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

package net.lshift.diffa.kernel.limiting

import org.easymock.EasyMock.{ createMock, expect, replay, reset }
import net.lshift.diffa.participant.changes.ChangeEvent
import org.joda.time.DateTime
import org.junit.{Before, Test, Assert}
import org.junit.runner.RunWith
import org.junit.experimental.theories.{Theories, DataPoints, Theory}
import net.lshift.diffa.kernel.config.limits.ChangeEventRate
import net.lshift.diffa.kernel.config.DomainServiceLimitsView

@RunWith(classOf[Theories])
class ChangeEventRateLimiterTest {
  import ChangeEventRateLimiterTest._

  var rateClock: Clock = createMock("rateClock", classOf[Clock])
  val rateLimitView = createMock("rateLimitView", classOf[DomainServiceLimitsView])
  var limiter: RateLimiter = _
  val event = ChangeEvent.forChange("id", "aaa111bbb222ffff", yesterday)
  val dummyDomain = "dummy"

  @Before
  def setup() {
    setClock(0L)
    setRateLimit(1)
    limiter = new RateLimiter(
      () => rateLimitView.getEffectiveLimitByNameForDomain(dummyDomain, ChangeEventRate),
      rateClock)
  }

  @Test
  def rateLimiterShouldAcceptFirstEvent() {
    Assert.assertTrue("First event should be accepted", limiter.accept())
  }

  @Theory
  def rateLimiterShouldRejectSubsequentEventsWhileLimited(scenario: Scenario) {
    limiter.accept()

    scenario.data match {
      case d: WithinLimitInterval =>
        setClock(d.relativeTime)
        Assert.assertFalse("Events received within limit window should be refused", limiter.accept())
      case _ =>
    }
  }

  @Theory
  def rateLimiterShouldAcceptSubsequentEventAfterLimitExpires(scenario: Scenario) {
    limiter.accept()

    scenario.data match {
      case _: WithinLimitInterval =>
      case after: AfterLimitInterval =>
        setClock(after.relativeTime)
        Assert.assertTrue("An event received after the limit expires should be accepted", limiter.accept())
    }
  }

  @Theory
  def allowedRateShouldMatchConfigurationAndRejectEventsWithinLimitWindow(scenario: Scenario) {
    limiter.accept()

    scenario.data match {
      case RateLimit(n) =>
        setRateLimit(n)
        setClock(1000 / n - 1)
        Assert.assertFalse("Event received within configured limit window should be refused", limiter.accept())
      case _ =>
    }
  }

  @Theory
  def allowedRateShouldMatchConfigurationAndAcceptEventsAfterLimitExpires(scenario: Scenario) {
    limiter.accept()

    scenario.data match {
      case RateLimit(n) =>
        setRateLimit(n)
        setClock(1000 / n + 1)
        Assert.assertTrue("Event received after configured limit window should be accepted", limiter.accept())
      case _ =>
    }
  }

  private def setRateLimit(eventsPerSecond: Int) {
    reset(rateLimitView)
    expect(rateLimitView.getEffectiveLimitByNameForDomain(dummyDomain, ChangeEventRate)).
      andReturn(1).anyTimes()
    replay(rateLimitView)
  }

  private def setClock(relativeTime: Long) {
    reset(rateClock)
    expect(rateClock.currentTimeMillis).andReturn(relativeTime).anyTimes()
    replay(rateClock)
  }
}

object ChangeEventRateLimiterTest {
  private[ChangeEventRateLimiterTest] val yesterday = (new DateTime) minusDays 1

  implicit def long2Data(l: Long): Data = if (l < 1000L) {
    WithinLimitInterval(l)
  } else {
    AfterLimitInterval(l)
  }

  @DataPoints
  def unacceptableEventTimes = List(
    Scenario(1L),
    Scenario(100L),
    Scenario(999L)
  ).toArray

  @DataPoints
  def acceptableTimes = List(
    Scenario(1000L),
    Scenario(1001L),
    Scenario(1100L)
  ).toArray

  @DataPoints
  def rateLimits = List(
    Scenario(RateLimit(1)),
    Scenario(RateLimit(2)),
    Scenario(RateLimit(3)),
    Scenario(RateLimit(4))
  )
}

case class Scenario(data: Data)
trait Data
case class WithinLimitInterval(relativeTime: Long) extends Data
case class AfterLimitInterval(relativeTime: Long) extends Data
case class RateLimit(ratePerSecond: Int) extends Data
