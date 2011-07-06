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

import org.junit.Test
import org.junit.Assert._
import support.TestEnvironment

trait CommonEscalationTests {

  def env:TestEnvironment

  @Test
  def canDeleteEscalation {
    def escalationName = env.escalationsClient.listEscalations(env.pairKey).headOption.map(_.name)
    assertEquals(Some(env.escalationName), escalationName)
    env.configurationClient.removeEscalation(env.escalationName, env.pairKey)
    assertEquals(None, escalationName)
  }
}