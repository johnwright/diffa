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
import net.lshift.diffa.kernel.differencing.SessionManager
import net.lshift.diffa.kernel.config.{ConfigStore, Pair}
import org.junit.runner.RunWith
import net.lshift.diffa.kernel.util.{Concurrent, ConcurrentJunitRunner}
import net.lshift.diffa.kernel.util.Concurrent._
import java.util.concurrent.{TimeUnit, LinkedBlockingQueue}

/**
 * Test cases for the QuartzScanScheduler.
 */
@RunWith(classOf[ConcurrentJunitRunner])
@Concurrent(threads = 20)
class QuartzScanSchedulerTest {
  val config = createStrictMock(classOf[ConfigStore])
  val sessions = createStrictMock(classOf[SessionManager])

  @Test
  def shouldAllowScheduleCreation() {
    val mb = createExecuteListenerQueue

    expect(config.listPairs).andReturn(Seq())
    expect(config.getPair("PairA")).andReturn(Pair(key = "PairA", scanCronSpec = generateNowishCronSpec))

    replayAll()

    withScheduler(new QuartzScanScheduler(config, sessions, "shouldAllowScheduleCreation")) { scheduler =>
      scheduler.onUpdatePair("PairA")
      
      mb.poll(3, TimeUnit.SECONDS) match {
        case null => fail("Scan was not triggered")
        case key:String => assertEquals("PairA", key)
      }
    }
  }

  @Test
  def shouldRestoreSchedulesOnStartup() {
    val mb = createExecuteListenerQueue

    expect(config.listPairs).andReturn(Seq(Pair(key = "PairB", scanCronSpec = generateNowishCronSpec)))

    replayAll()

    withScheduler(new QuartzScanScheduler(config, sessions, "shouldRestoreSchedulesOnStartup")) { scheduler =>
      mb.poll(3, TimeUnit.SECONDS) match {
        case null => fail("Scan was not triggered")
        case key:String => assertEquals("PairB", key)
      }
    }
  }

  @Test
  def shouldAllowSchedulesToBeDeleted() {
    val mb = createExecuteListenerQueue

    expect(config.listPairs).andReturn(Seq(Pair(key = "PairC", scanCronSpec = generateNowishCronSpec)))

    replayAll()

    withScheduler(new QuartzScanScheduler(config, sessions, "shouldAllowSchedulesToBeDeleted")) { scheduler =>
      scheduler.onDeletePair("PairC")

      mb.poll(3, TimeUnit.SECONDS) match {
        case null =>
        case key:String => fail("Scheduler should not have started scan for pair " + key)
      }
    }
  }

  @Test
  def shouldAllowSchedulesToBeUpdated() {
    val mb = createExecuteListenerQueue

    expect(config.listPairs).andReturn(Seq())
    expect(config.getPair("PairD")).andReturn(Pair(key = "PairD", scanCronSpec = generateOldCronSpec)).once()
    expect(config.getPair("PairD")).andReturn(Pair(key = "PairD", scanCronSpec = generateNowishCronSpec)).once()

    replayAll()

    // Initially schedule with something too old to run, then update it with something new enough that will
    withScheduler(new QuartzScanScheduler(config, sessions, "shouldAllowSchedulesToBeUpdated")) { scheduler =>
      scheduler.onUpdatePair("PairD")   // We'll get a different pair result on each call
      scheduler.onUpdatePair("PairD")

      mb.poll(5, TimeUnit.SECONDS) match {
        case null => fail("Scan was not triggered")
        case key:String => assertEquals("PairD", key)
      }

      verify(config, sessions)
    }
  }

  private def replayAll() { replay(config, sessions) }

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
    val q = new LinkedBlockingQueue[String]
    expect(sessions.runScanForPair(anyObject.asInstanceOf[String])).andAnswer(new IAnswer[Unit] {
      def answer = {
        val pairKey = getCurrentArguments()(0).asInstanceOf[String]
        q.add(pairKey)
      }
    })

    q
  }
}