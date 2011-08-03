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

package net.lshift.diffa.kernel.notifications

import org.easymock.EasyMock._
import org.junit.Assert._
import net.lshift.diffa.kernel.events.VersionID
import org.junit.{Before, Test}
import org.joda.time.{Period, DateTime}
import net.lshift.diffa.kernel.differencing.{LiveWindow, SessionManager}
import scala.collection.JavaConversions._
import net.lshift.diffa.kernel.config.{Domain, User}
import collection.mutable.HashSet
import net.lshift.diffa.kernel.config.system.SystemConfigStore

class EventNotifierTest {

  val domain = "domain"
  val systemConfigStore = createStrictMock("configStore", classOf[SystemConfigStore])
  val sessionManager = createStrictMock("sessionManager", classOf[SessionManager])

  val quietTimeMillis = 2000
  val notifier = new EventNotifier(sessionManager, systemConfigStore, Period.millis(quietTimeMillis))

  @Before
  def setup = {
    val user = User("Foo Bar","dev_null@lshift.net")
    expect(systemConfigStore.listUsers).andStubReturn(List(user))
    replay(systemConfigStore, sessionManager)
  }

  @Test
  def quiteTime = {
    val id = VersionID(pairKey = "pair", domain = domain, id = "abc")
    val timestamp = new DateTime()
    val up = "foo"
    val down = "bar"

    var notifications = 0

    val provider = new NotificationProvider() {
      def notify(event:NotificationEvent, user:User) = {
        notifications += 1
      }
    }

    // On TC this seems to be timing sensitive
    Thread.sleep(quietTimeMillis)

    notifier.registerProvider(provider)
    notifier.onMismatch(id, timestamp, up, down, LiveWindow)
    assertEquals(1,notifications)
    notifier.onMismatch(id, timestamp, up, down, LiveWindow)
    assertEquals(1,notifications)
    Thread.sleep(quietTimeMillis * 110/100)
    notifier.onMismatch(id, timestamp, up, down, LiveWindow)
    assertEquals(2,notifications)
  }
}