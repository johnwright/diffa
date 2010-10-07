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

package net.lshift.diffa.agent.itest.support

/**
 * Static set of environments that can be used in integration test cases.
 */
object TestEnvironments {
  lazy val abSame = new TestEnvironment("abSame", 20094, 20095, SameVersionScheme)
  lazy val abCorrelated = new TestEnvironment("abCorrelated", 20194, 20195, CorrelatedVersionScheme)
}