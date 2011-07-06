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

package net.lshift.diffa.kernel.participants

import org.junit.Test
import org.junit.Assert._

import net.lshift.diffa.kernel.participants.IntegerCategoryFunction._

class IntegerPartitionTest {

  protected val tens = IntegerCategoryFunction("someInt", 10, 10)
  protected val hundreds = IntegerCategoryFunction("someInt", 100, 10)

  val constraint = new QueryConstraint {
    def category = "someInt"
    def wireFormat = null
  }

  val constraint2 = new QueryConstraint {
    def category = "someInt2"
    def wireFormat = null
  }

  val constraint3 = new QueryConstraint {
    def category = "someInt3"
    def wireFormat = null
  }

  @Test
  def tensPartition {
    assertEquals("120", tens.bucket("123"))
    assertEquals("10", tens.bucket("12"))
    assertEquals("0", tens.bucket("1"))
  }

  @Test
  def hundredsPartition {
    assertEquals("1200", hundreds.bucket("1234"))
    assertEquals("100", hundreds.bucket("123"))
    assertEquals("0", hundreds.bucket("12"))
    assertEquals("0", hundreds.bucket("1"))
  }

  @Test
  def autoDescendingIntegerCategoryFunction {
    def binaryCategoryFunction(denom: Int) = IntegerCategoryFunction("someInt", denom, 2)
    val myBinaryCategoryFunction = binaryCategoryFunction(128)
    assertEquals("256", myBinaryCategoryFunction.bucket("300"))
    assertEquals(IntegerRangeConstraint("someInt", 256, 383),
                 myBinaryCategoryFunction.constrain(constraint, "256"))
    assertEquals(Some(binaryCategoryFunction(64)), myBinaryCategoryFunction.descend)
  }

  @Test
  def descendFromTensPartition {
    assertEquals(None, tens.descend)
    assertEquals(IntegerRangeConstraint("someInt", 10, 19), tens.constrain(constraint, "10"))
  }

  @Test
  def descendFromHundredsPartition {
    assertEquals(Some(tens), hundreds.descend)
    assertEquals(IntegerRangeConstraint("someInt2", 100, 199), hundreds.constrain(constraint2, "100"))
  }

  @Test(expected=classOf[InvalidAttributeValueException])
  def shouldThrowInvalidCategoryExceptionIfValueIsNotInteger {
    tens.owningPartition("NOT_AN_INTEGER")
  }

  @Test(expected=classOf[InvalidAttributeValueException])
  def descendShouldThrowInvalidAttributeValueExceptionIfPartitionValueIsInvalid {
    tens.constrain(constraint3, "123")
  }

  @Test(expected=classOf[IllegalArgumentException])
  def autoDescendingIntegerCategoryShouldThrowIllegalArgumentExceptionIfInstantiatedWithInvalidArgs {
    new IntegerCategoryFunction("someInt", 100, 3) // 3 is not a factor of 100
  }

}
