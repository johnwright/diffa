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

package net.lshift.diffa.agent.itest

import support.TestEnvironment
import org.junit.Assert._
import net.lshift.diffa.agent.itest.support.TestConstants._
import net.lshift.diffa.kernel.client.ActionableRequest
import net.lshift.diffa.messaging.json.BadRequestException
import org.junit._

trait CommonActionTests {

  def env:TestEnvironment

  @Before
  def startActionServer {
    env.repairActionsComponent.start()
  }

  @After
  def stopActionServer {
    env.repairActionsComponent.stop()
  }

  @Test
  def shouldListEntityScopedActions {
    val actions = env.actionsClient.listEntityScopedActions(env.pairKey)
    assertNotNull(actions)
    assertEquals(1, actions.size)
    assertEquals("Resend Source", actions(0).name)
  }

  @Test
  def invokeEntityScopedAction {
    val request = ActionableRequest(env.pairKey, env.entityScopedActionName, "abc")
    val response = env.actionsClient.invoke(request)
    assertNotNull(response)
    assertEquals("success", response.result)
  }

  @Test
  def shouldListPairScopedActions {
    env.createPairScopedAction
    val actions = env.actionsClient.listPairScopedActions(env.pairKey)
    assertNotNull(actions)
    assertEquals(Some(env.pairScopedActionName), actions.headOption.map(_.name))
  }

  @Test
  def invokePairScopedAction {
    env.createPairScopedAction
    val request = ActionableRequest(env.pairKey, env.pairScopedActionName, null)
    val response = env.actionsClient.invoke(request)
    assertNotNull(response)
    assertEquals("success", response.result)
    assertEquals("resending all", response.output)
  }

  @Test
  def canDeleteAction {
    def actionName = env.actionsClient.listEntityScopedActions(env.pairKey).headOption.map(_.name)
    assertEquals(Some(env.entityScopedActionName), actionName)
    env.configurationClient.removeRepairAction(env.entityScopedActionName, env.pairKey)
    assertEquals(None, actionName)
  }

  @Test(expected=classOf[BadRequestException])
  def shouldRejectInvalidActionScope {
    env.configurationClient.declareRepairAction(env.entityScopedActionName, "resend", "INVALID SCOPE", env.pairKey)
  }

}