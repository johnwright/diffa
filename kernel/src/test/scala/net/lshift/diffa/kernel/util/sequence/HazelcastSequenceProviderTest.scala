/**
 * Copyright (C) 2010-2012 LShift Ltd.
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
package net.lshift.diffa.kernel.util.sequence

import org.junit.Test
import org.junit.Assert._
import com.eaio.uuid.UUID

class HazelcastSequenceProviderTest {

  val provider = new HazelcastSequenceProvider


  @Test
  def shouldSetSequenceGivenKnowledgeOfTheCurrentValue {
    val id  = "HazelcastSequenceProviderTest." + new UUID().toString

    // Given that we know what the current value is
    assertEquals(0, provider.currentSequenceValue(id))

    // We should be able to upgrade the value
    assertTrue(provider.upgradeSequenceValue(id, 0, 100))
  }

  @Test
  def shouldNotSetSequenceWithoutKnowledgeOfTheCurrentValue {
    val id  = "HazelcastSequenceProviderTest." + new UUID().toString

    // Given that we know what the current value _really_is
    assertEquals(0, provider.currentSequenceValue(id))

    // We can pretend we don't know what the actual value is
    // and hence we should not be able to upgrade the value from a bogus
    assertFalse(provider.upgradeSequenceValue(id, 1, 100))
  }

  @Test
  def shouldIncrementSequenceValue {

    val id  = "HazelcastSequenceProviderTest." + new UUID().toString

    // Given that we know what the current value is
    assertEquals(0, provider.currentSequenceValue(id))

    // We can increment the value and test the result
    assertEquals(1, provider.nextSequenceValue(id))

  }

}
