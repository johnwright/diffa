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

package net.lshift.diffa.kernel.escalation

import org.junit.Test
import org.easymock.EasyMock._
import org.easymock.EasyMock
import net.lshift.diffa.kernel.config.{RepairAction, ConfigStore, Pair => Pair}
import net.lshift.diffa.kernel.client.{ActionableRequest, ActionsClient}
import net.lshift.diffa.kernel.events.VersionID
import net.lshift.diffa.kernel.differencing.ScanTrigger
import org.joda.time.DateTime
import net.lshift.diffa.kernel.frontend.wire.InvocationResult

class EscalationManagerTest {

  var pairKey = "some pair key"

  val configStore = createMock(classOf[ConfigStore])
  val actionsClient = createStrictMock(classOf[ActionsClient])

  expect(configStore.getPair(pairKey)).andReturn(Pair())

  expect(configStore.listEscalationsForPair(EasyMock.isA(classOf[Pair]))).andReturn(
    List(RepairAction("foo", "url", RepairAction.ENTITY_SCOPE, pairKey, true))
  )

  expect(actionsClient.invoke(EasyMock.isA(classOf[ActionableRequest]))).andReturn(InvocationResult("200", "Success"))

  replay(configStore, actionsClient)

  val escalationManager = new EscalationManager(configStore, actionsClient)

  @Test
  def shouldEscalateActionsAfterScan = {

    escalationManager.onMismatch(VersionID(pairKey, "id"), new DateTime(), "uvsn", "dvsn", ScanTrigger)

    verify(configStore, actionsClient)
  }

}