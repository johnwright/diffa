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

package net.lshift.diffa.kernel.frontend

import org.junit.Assert._
import org.junit.Test

class ConfigurationTest {

  def doAuth(kernel: Configuration, user:String,pass:String,expected:Int): Unit = {
    val response = kernel.auth(new AuthRequest(user, pass))
    assertEquals(expected, response.response)
  }

  // To be replaced with proper tests
  @Test
  def auth() {
    val kernel = new Configuration(null, null, null)
    doAuth(kernel, "foo", "bar", 0)
    doAuth(kernel, "admin", "admin", 1)
  }
}