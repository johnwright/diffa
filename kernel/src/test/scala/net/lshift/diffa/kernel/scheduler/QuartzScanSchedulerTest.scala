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
import concurrent.{TIMEOUT, MailBox}
import net.lshift.diffa.kernel.config.{GroupContainer, ConfigStore, Pair}
import org.junit.runner.RunWith
import net.lshift.diffa.kernel.util.{Concurrent, ConcurrentJunitRunner}
import net.lshift.diffa.kernel.util.Concurrent._

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
    val mb = createExecuteListenerMb
    
    expect(config.listGroups).andReturn(Seq())
    expect(config.getPair("PairA")).andReturn(Pair(key = "PairA", scanCronSpec = generateNowishCronSpec))
    replayAll()

    withScheduler(new QuartzScanScheduler(config, sessions, "shouldAllowScheduleCreation")) { scheduler =>
      scheduler.onUpdatePair("PairA")
      
      mb.receiveWithin(3000) {
        case TIMEOUT => fail("Scan was not triggered")
        case key:String => assertEquals("PairA", key)
      }
    }
  }

  @Test
  def shouldRestoreSchedulesOnStartup() {
    val mb = createExecuteListenerMb
    
    expect(config.listGroups).andReturn(Seq(GroupContainer(null, Array(Pair(key = "PairB", scanCronSpec = generateNowishCronSpec)))))
    replayAll()

    withScheduler(new QuartzScanScheduler(config, sessions, "shouldRestoreSchedulesOnStartup")) { scheduler =>
      mb.receiveWithin(3000) {
        case TIMEOUT => fail("Scan was not triggered")
        case key:String => assertEquals("PairB", key)
      }
    }
  }

  @Test
  def shouldAllowSchedulesToBeDeleted() {
    val mb = createExecuteListenerMb

    expect(config.listGroups).andReturn(Seq(GroupContainer(null, Array(Pair(key = "PairC", scanCronSpec = generateNowishCronSpec)))))
    replayAll()

    withScheduler(new QuartzScanScheduler(config, sessions, "shouldAllowSchedulesToBeDeleted")) { scheduler =>
      scheduler.onDeletePair("PairC")

      mb.receiveWithin(3000) {
        case TIMEOUT =>
        case key:String => fail("Scheduler should not have started scan for pair " + key)
      }
    }
  }

  @Test
  def shouldAllowSchedulesToBeUpdated() {
    val mb = createExecuteListenerMb

    expect(config.listGroups).andReturn(Seq())
    expect(config.getPair("PairD")).andReturn(Pair(key = "PairD", scanCronSpec = generateOldCronSpec)).once()
    expect(config.getPair("PairD")).andReturn(Pair(key = "PairD", scanCronSpec = generateNowishCronSpec)).once()
    replayAll()

    // Initially schedule with something too old to run, then update it with something new enough that will
    withScheduler(new QuartzScanScheduler(config, sessions, "shouldAllowSchedulesToBeUpdated")) { scheduler =>
      scheduler.onUpdatePair("PairD")   // We'll get a different pair result on each call
      scheduler.onUpdatePair("PairD")

      mb.receiveWithin(3000) {
        case TIMEOUT => fail("Scan was not triggered")
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

  private def createExecuteListenerMb = {
    val mb = new MailBox
    expect(sessions.runScanForPair(anyObject.asInstanceOf[String])).andAnswer(new IAnswer[Unit] {
      def answer = {
        val pairKey = getCurrentArguments()(0)
        mb.send(pairKey)
      }
    })

    mb
  }
}