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
import net.lshift.diffa.kernel.frontend.wire.WireConstraint
import scala.collection.JavaConversions._

class StringPartitionTest {

  val constraint = new QueryConstraint {
    def category = "foo"
    def wireFormat = null
  }

  @Test
  def owningPartitionReturnsShortestPrefix {
    val spc = new StringPrefixCategoryFunction(prefixLength = 2, maxLength = 1, step = 1)
    assertEquals("", spc.owningPartition(""))
    assertEquals("x", spc.owningPartition("x"))
    assertEquals("xx", spc.owningPartition("xx"))
    assertEquals("xx", spc.owningPartition("xxx"))
  }

  @Test
  def descendsFromPrefixOfLengthOneWithStepOne {
    val spc = new StringPrefixCategoryFunction(prefixLength = 1, maxLength = 2, step = 1)
    assertEquals(Some(StringPrefixCategoryFunction(prefixLength = 2, maxLength = 2, step = 1)),
                 spc.descend)
  }

  @Test
  def descendsFromPrefixOfLengthOneWithStepTwo {
    val spc = new StringPrefixCategoryFunction(prefixLength = 1, maxLength = 3, step = 2)
    assertEquals(Some(StringPrefixCategoryFunction(prefixLength = 3, maxLength = 3, step = 2)),
                 spc.descend)
  }

  @Test
  def descendsToIndividualWhenPrefixIsSameAsMaxLength {
    val spc = new StringPrefixCategoryFunction(prefixLength = 2, maxLength = 2, step = 1)
    assertEquals(Some(IndividualCategoryFunction),
                spc.descend)
  }

  @Test
  def descendsToMaxLengthWhenStepAboutToExceed {
    val spc = new StringPrefixCategoryFunction(prefixLength = 1, maxLength = 2, step = 2)
    assertEquals(Some(StringPrefixCategoryFunction(prefixLength = 2, maxLength = 2, step = 2)),
                 spc.descend)
  }

  @Test
  def generatesConstraintForPartition {
    val spc = StringPrefixCategoryFunction(prefixLength = 2, maxLength = 2, step = 1)
    assertEquals(PrefixQueryConstraint("foo", "xx"), spc.constrain(constraint, "xx"))
  }

  @Test
  def cannotConstrainWhenPartitionIsTooShort {
    val spc = StringPrefixCategoryFunction(prefixLength = 2, maxLength = 2, step = 1)
    assertEquals(SetQueryConstraint("foo", Set("x")), spc.constrain(constraint, "x"))
  }

  @Test(expected = classOf[InvalidAttributeValueException])
  def cannotConstrainWhenPartitionIsTooLong {
    val spc = StringPrefixCategoryFunction(prefixLength = 2, maxLength = 2, step = 1)
    spc.constrain(constraint, "xxx")
  }

  @Test
  def shouldBucketIsAlwaysTrue {
    assertTrue(new StringPrefixCategoryFunction(1, 1, 1).shouldBucket)
  }

  @Test
  def nameShouldBeQuestionMarksFollowedByStar {
    assertEquals("prefix(2,1,1)", new StringPrefixCategoryFunction(2, 1, 1).name)
  }

  @Test
  def prefixQueryConstraintMustImplementWireFormat {
    assertEquals(WireConstraint("foo", Map("prefix" -> "abc"), null),
               PrefixQueryConstraint("foo", "abc").wireFormat)
  }

  @Test
  def parsesFromName {
    assertEquals(StringPrefixCategoryFunction(1, 1, 1),
               StringPrefixCategoryFunction.parse("prefix(1,1,1)"))
    assertEquals(StringPrefixCategoryFunction(10, 100, 10),
               StringPrefixCategoryFunction.parse("prefix(10,100,10)"))
  }

  @Test(expected = classOf[IllegalArgumentException])
  def failsToParseEmptyString {
    StringPrefixCategoryFunction.parse("")
  }

  @Test(expected = classOf[IllegalArgumentException])
  def failsToParseNonDigitValues {
    StringPrefixCategoryFunction.parse("prefix(a,b,c)")
  }

}