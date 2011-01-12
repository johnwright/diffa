/**
 * Copyright (C) 2011 LShift Ltd.
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

package net.lshift.diffa.kernel.participants

import org.junit.Test
import org.junit.Assert._

class IntegerPartitionTest {

  @Test
  def tensPartition {
    assertEquals("12", TensCategoryFunction.owningPartition("123"))
    assertEquals("1", TensCategoryFunction.owningPartition("12"))
    assertEquals("0", TensCategoryFunction.owningPartition("1"))
  }

  @Test
  def hundredsPartition {
    assertEquals("12", HundredsCategoryFunction.owningPartition("1234"))
    assertEquals("1", HundredsCategoryFunction.owningPartition("123"))
    assertEquals("0", HundredsCategoryFunction.owningPartition("12"))
    assertEquals("0", HundredsCategoryFunction.owningPartition("1"))

  }

  @Test
  def thousandsPartition {
    assertEquals("12", ThousandsCategoryFunction.owningPartition("12345"))
    assertEquals("1", ThousandsCategoryFunction.owningPartition("1234"))
    assertEquals("0", ThousandsCategoryFunction.owningPartition("123"))
    assertEquals("0", ThousandsCategoryFunction.owningPartition("12"))
    assertEquals("0", ThousandsCategoryFunction.owningPartition("1"))
  }

}
