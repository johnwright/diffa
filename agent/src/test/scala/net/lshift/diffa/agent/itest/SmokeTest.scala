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

import org.junit.Assert._
import org.junit.Test
import net.lshift.diffa.messaging.json.ConfigurationRestClient

/**
 * Smoke tests for simple operations that can be performed against the agent. Intended to provide quick diagnostic
 * if basic functions become broken somehow.
 */
class SmokeTest {

  val configRestClient = new ConfigurationRestClient("http://localhost:19093/diffa-agent")

  @Test
  def auth = {
    assertEquals(0, configRestClient.auth("mr.fawlty", "que"))
    assertEquals(1, configRestClient.auth("admin", "admin"))
  }

}

