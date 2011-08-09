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

package net.lshift.diffa.kernel.scheduler

import org.junit.Test
import org.easymock.EasyMock._
import org.junit.Assert._
import org.joda.time.DateTime
import org.easymock.IAnswer
import org.junit.runner.RunWith
import net.lshift.diffa.kernel.util.{Concurrent, ConcurrentJunitRunner}
import java.util.concurrent.{TimeUnit, LinkedBlockingQueue}
import net.lshift.diffa.kernel.actors.PairPolicyClient
import net.lshift.diffa.kernel.config.system.SystemConfigStore
import net.lshift.diffa.kernel.config.{DiffaPairRef, Domain, Pair => DiffaPair}

/**
 * Test cases for the QuartzScanScheduler.
 */
@RunWith(classOf[ConcurrentJunitRunner])
@Concurrent(threads = 20)
class QuartzScanSchedulerTest {
  val systemConfig = createStrictMock(classOf[SystemConfigStore])
  val pairPolicyClient = createStrictMock(classOf[PairPolicyClient])

  val domain = Domain(name="domain")

  @Test
  def shouldAllowScheduleCreation() {
    val mb = createExecuteListenerQueue

    val pair = DiffaPair(key = "PairA", domain=domain, scanCronSpec = generateNowishCronSpec)

    expect(systemConfig.listPairs).andReturn(Seq())

    replayAll()

    withScheduler(new QuartzScanScheduler(systemConfig, pairPolicyClient, "shouldAllowScheduleCreation")) { scheduler =>
      scheduler.onUpdatePair(pair)

      mb.poll(3, TimeUnit.SECONDS) match {
        case null => fail("Scan was not triggered")
        case p:DiffaPairRef => assertEquals("PairA", p.key)
      }
    }
  }

  @Test
  def shouldRestoreSchedulesOnStartup() {
    val mb = createExecuteListenerQueue

    val pair = DiffaPair(key = "PairB", domain=domain, scanCronSpec = generateNowishCronSpec)

    expect(systemConfig.listPairs).andReturn(Seq(pair))
    expect(systemConfig.getPair("domain", "PairB")).andReturn(pair)

    replayAll()

    withScheduler(new QuartzScanScheduler(systemConfig, pairPolicyClient, "shouldRestoreSchedulesOnStartup")) { scheduler =>
      mb.poll(3, TimeUnit.SECONDS) match {
        case null => fail("Scan was not triggered")
        case pair:DiffaPairRef => assertEquals("PairB", pair.key)
      }
    }
  }

  @Test
  def shouldAllowSchedulesToBeDeleted() {
    val mb = createExecuteListenerQueue

    val pair = DiffaPair(key = "PairC", domain=domain, scanCronSpec = generateNowishCronSpec)

    expect(systemConfig.listPairs).andReturn(Seq(pair))

    replayAll()

    withScheduler(new QuartzScanScheduler(systemConfig, pairPolicyClient, "shouldAllowSchedulesToBeDeleted")) { scheduler =>
      scheduler.onDeletePair(pair)

      mb.poll(3, TimeUnit.SECONDS) match {
        case null =>
        case p:DiffaPairRef => fail("Scheduler should not have started scan for pair " + p)
      }
    }
  }

  @Test
  def shouldAllowSchedulesToBeUpdated() {
    val mb = createExecuteListenerQueue

    val p1 = DiffaPair(key = "PairD", domain=domain, scanCronSpec = generateOldCronSpec)
    val p2 = DiffaPair(key = "PairD", domain=domain, scanCronSpec = generateNowishCronSpec)

    expect(systemConfig.listPairs).andReturn(Seq())

    replayAll()

    // Initially schedule with something too old to run, then update it with something new enough that will
    withScheduler(new QuartzScanScheduler(systemConfig, pairPolicyClient, "shouldAllowSchedulesToBeUpdated")) { scheduler =>
      scheduler.onUpdatePair(p1)   // We'll get a different pair result on each call
      scheduler.onUpdatePair(p2)

      mb.poll(5, TimeUnit.SECONDS) match {
        case null => fail("Scan was not triggered")
        case p:DiffaPairRef => assertEquals("PairD", p.key)
      }

      verify(systemConfig, pairPolicyClient)
    }
  }

  private def replayAll() { replay(systemConfig, pairPolicyClient) }

  private def withScheduler[T](s:QuartzScanScheduler)(f:(QuartzScanScheduler) => T) {
    try {
      f(s)
    } finally {
      s.close()
    }
  }

  private def generateNowishCronSpec = {
    val nowish = (new DateTime).plusSeconds(2)
    nowish.getSecondOfMinute + " " + nowish.getMinuteOfHour + " " + nowish.getHourOfDay + " * * ?"
  }

  private def generateOldCronSpec = {
    val nowish = (new DateTime).minusHours(1)
    nowish.getSecondOfMinute + " " + nowish.getMinuteOfHour + " " + nowish.getHourOfDay + " * * ?"
  }

  private def createExecuteListenerQueue = {
    val q = new LinkedBlockingQueue[DiffaPairRef]
    expect(pairPolicyClient.scanPair(anyObject.asInstanceOf[DiffaPairRef])).andAnswer(new IAnswer[Unit] {
      def answer = {
        val pair = getCurrentArguments()(0).asInstanceOf[DiffaPairRef]
        q.add(pair)
      }
    })

    q
  }
}