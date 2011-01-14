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
    assertEquals("120", TensCategoryFunction.owningPartition("123"))
    assertEquals("10", TensCategoryFunction.owningPartition("12"))
    assertEquals("0", TensCategoryFunction.owningPartition("1"))
  }

  @Test
  def hundredsPartition {
    assertEquals("1200", HundredsCategoryFunction.owningPartition("1234"))
    assertEquals("100", HundredsCategoryFunction.owningPartition("123"))
    assertEquals("0", HundredsCategoryFunction.owningPartition("12"))
    assertEquals("0", HundredsCategoryFunction.owningPartition("1"))
  }

  @Test
  def thousandsPartition {
    assertEquals("12000", ThousandsCategoryFunction.owningPartition("12345"))
    assertEquals("1000", ThousandsCategoryFunction.owningPartition("1234"))
    assertEquals("0", ThousandsCategoryFunction.owningPartition("123"))
    assertEquals("0", ThousandsCategoryFunction.owningPartition("12"))
    assertEquals("0", ThousandsCategoryFunction.owningPartition("1"))
  }

  @Test
  def arbitraryPartition {
    object ArbitraryCategoryFunction extends IntegerCategoryFunction("1337s", 1337, IndividualCategoryFunction)
    assertEquals("2674", ArbitraryCategoryFunction.owningPartition("3456"))
  }

  @Test
  def descendFromTensPartition {
    assertEquals(Some(IntermediateResult("10", "19", IndividualCategoryFunction)),
                 TensCategoryFunction.descend("10"))
  }

  @Test
  def descendFromHundredsPartition {
    assertEquals(Some(IntermediateResult("100", "199", TensCategoryFunction)),
                 HundredsCategoryFunction.descend("100"))
  }

  @Test
  def descendFromThousandsPartition {
    assertEquals(Some(IntermediateResult("1000", "1999", HundredsCategoryFunction)),
                 ThousandsCategoryFunction.descend("1000"))
  }

  @Test(expected=classOf[InvalidCategoryException])
  def shouldThrowInvalidCategoryExceptionIfValueIsNotInteger {
    TensCategoryFunction.owningPartition("NOT_AN_INTEGER")
  }

}
