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

package net.lshift.diffa.agent.itest

import support.TestEnvironment
import org.junit.Test
import org.junit.Assert._
import net.lshift.diffa.agent.itest.support.TestConstants._
import net.lshift.diffa.kernel.client.ActionRequest

trait CommonActionTests {


  def env:TestEnvironment


  @Test
  def shouldHaveActions = {
    val actions = env.actionsClient.listActions(env.pairKey)
    assertNotNull(actions)
    assertEquals(1, actions.size)
    assertEquals("resend", actions(0).id)
  }

  @Test
  def invokeAction = {
    val entityId = "abc"
    env.upstream.addEntity(entityId, env.bizDateValues(yesterday), yesterday, "abcdef")
    //env.upstream.addEntity(entityId, yesterday, yesterday, "abcdef")
    val pairKey = env.pairKey
    val actionId = "resend"    
    val request = ActionRequest(pairKey, actionId, entityId)
    val response = env.actionsClient.invoke(request)
    assertNotNull(response)
    assertEquals("success", response.result)    
  }
}