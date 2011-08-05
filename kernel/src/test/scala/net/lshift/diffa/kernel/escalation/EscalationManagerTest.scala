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

import org.easymock.EasyMock._
import org.easymock.EasyMock
import net.lshift.diffa.kernel.client.{ActionableRequest, ActionsClient}
import net.lshift.diffa.kernel.events.VersionID
import org.joda.time.DateTime
import net.lshift.diffa.kernel.frontend.wire.InvocationResult
import org.junit.runner.RunWith
import net.lshift.diffa.kernel.differencing.{MatchOrigin, LiveWindow, TriggeredByScan}
import net.lshift.diffa.kernel.escalation.EscalationManagerTest.Scenario
import net.lshift.diffa.kernel.config._
import org.junit.experimental.theories.{DataPoints, DataPoint, Theories, Theory}
import net.lshift.diffa.kernel.config.{Pair => DiffaPair}
import net.lshift.diffa.kernel.frontend.EscalationDef

@RunWith(classOf[Theories])
class EscalationManagerTest {

  val domain = "domain"
  val pairKey = "some pair key"
  val pair = DiffaPair(key = pairKey, domain = Domain(name=domain))

  val configStore = createMock(classOf[DomainConfigStore])
  val actionsClient = createStrictMock(classOf[ActionsClient])
  val escalationManager = new EscalationManager(configStore, actionsClient)

  def expectConfigStore(event:String) = {

    expect(configStore.listEscalationsForPair(domain, pairKey)).andReturn(
      List(EscalationDef("foo", pairKey, "bar", EscalationActionType.REPAIR, event, EscalationOrigin.SCAN))
    ).anyTimes()

    replay(configStore)
  }

  def expectActionsClient(count:Int) = {
    if (count > 0) {
      expect(actionsClient.invoke(EasyMock.isA(classOf[ActionableRequest]))).andReturn(InvocationResult("200", "Success")).times(count)
    }
    replay(actionsClient)
  }

  @Theory
  def escalationsSometimesTriggerActions(scenario:Scenario) = {

    expectConfigStore(scenario.event)
    expectActionsClient(scenario.invocations)

    escalationManager.onMismatch(VersionID(pair.asRef, "id"), new DateTime(), scenario.uvsn, scenario.dvsn, scenario.matchOrigin)

    verify(configStore, actionsClient)
  }
}

object EscalationManagerTest {

  case class Scenario(uvsn:String,
                      dvsn:String,
                      event:String,
                      matchOrigin:MatchOrigin,
                      invocations:Int)

  @DataPoints def mismatchShouldBeEscalated = Array (
    Scenario("uvsn", "dsvn", EscalationEvent.MISMATCH, TriggeredByScan, 1),
    Scenario("uvsn", "", EscalationEvent.MISMATCH, TriggeredByScan, 0),
    Scenario("", "dsvn", EscalationEvent.MISMATCH, TriggeredByScan, 0)
  )

  @DataPoints def missingDownstreamShouldBeEscalated = Array (
    Scenario("uvsn", "", EscalationEvent.DOWNSTREAM_MISSING, TriggeredByScan, 1),
    Scenario("uvsn", "dvsn", EscalationEvent.DOWNSTREAM_MISSING, TriggeredByScan, 0),
    Scenario("", "dvsn", EscalationEvent.DOWNSTREAM_MISSING, TriggeredByScan, 0)
  )

  @DataPoints def missingUpstreamShouldBeEscalated = Array (
    Scenario("uvsn", "", EscalationEvent.UPSTREAM_MISSING, TriggeredByScan, 0),
    Scenario("uvsn", "dvsn", EscalationEvent.UPSTREAM_MISSING, TriggeredByScan, 0),
    Scenario("", "dvsn", EscalationEvent.UPSTREAM_MISSING, TriggeredByScan, 1)
  )

  @DataPoint def liveWindowShouldNotGetEscalated =
    Scenario("uvsn", "dvsn", EscalationEvent.MISMATCH, LiveWindow, 0)

}