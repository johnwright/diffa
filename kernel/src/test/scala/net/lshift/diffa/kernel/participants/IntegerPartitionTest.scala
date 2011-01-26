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

  protected val tens = AutoNarrowingIntegerCategoryFunction(10, 10)
  protected val hundreds = AutoNarrowingIntegerCategoryFunction(100, 10)

  @Test
  def tensPartition {
    assertEquals("120", tens.owningPartition("123"))
    assertEquals("10", tens.owningPartition("12"))
    assertEquals("0", tens.owningPartition("1"))
  }

  @Test
  def hundredsPartition {
    assertEquals("1200", hundreds.owningPartition("1234"))
    assertEquals("100", hundreds.owningPartition("123"))
    assertEquals("0", hundreds.owningPartition("12"))
    assertEquals("0", hundreds.owningPartition("1"))
  }

  @Test
  def arbitraryPartition {
    object ArbitraryCategoryFunction extends IntegerCategoryFunction(1337) {
      def name = "arbitrary-1337"
      def descend = Some(IndividualCategoryFunction)
    }
    assertEquals("2674", ArbitraryCategoryFunction.owningPartition("3456"))
  }

  @Test
  def autoDescendingIntegerCategoryFunction {
    def binaryCategoryFunction(denom: Int) = AutoNarrowingIntegerCategoryFunction(denom, 2)
    val myBinaryCategoryFunction = binaryCategoryFunction(128)
    assertEquals("256", myBinaryCategoryFunction.owningPartition("300"))
    assertEquals(IntegerRangeConstraint("someInt", 256, 383),
                 myBinaryCategoryFunction.constrain("someInt", "256"))
    assertEquals(Some(binaryCategoryFunction(64)), myBinaryCategoryFunction.descend)
  }

  @Test
  def descendFromTensPartition {
    assertEquals(Some(IndividualCategoryFunction), tens.descend)
    assertEquals(IntegerRangeConstraint("someInt", 10, 19), tens.constrain("someInt", "10"))
  }

  @Test
  def descendFromHundredsPartition {
    assertEquals(Some(tens), hundreds.descend)
    assertEquals(IntegerRangeConstraint("someInt2", 100, 199), hundreds.constrain("someInt2", "100"))
  }

  @Test(expected=classOf[InvalidAttributeValueException])
  def shouldThrowInvalidCategoryExceptionIfValueIsNotInteger {
    tens.owningPartition("NOT_AN_INTEGER")
  }

  @Test(expected=classOf[InvalidAttributeValueException])
  def descendShouldThrowInvalidAttributeValueExceptionIfPartitionValueIsInvalid {
    tens.constrain("someInt3", "123")
  }

  @Test(expected=classOf[IllegalArgumentException])
  def autoDescendingIntegerCategoryShouldThrowIllegalArgumentExceptionIfInstantiatedWithInvalidArgs {
    new AutoNarrowingIntegerCategoryFunction(100, 3) // 3 is not a factor of 100
  }

}
