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
import scala.collection.JavaConversions._
import net.lshift.diffa.participant.scanning.{InvalidAttributeValueException, ScanConstraint, SetConstraint, StringPrefixConstraint}

class StringPartitionTest {

  @Test
  def owningPartitionReturnsShortestPrefix {
    val spc = StringPrefixCategoryFunction("ss", prefixLength = 2, maxLength = 1, step = 1)
    assertEquals("", spc.bucket(""))
    assertEquals("x", spc.bucket("x"))
    assertEquals("xx", spc.bucket("xx"))
    assertEquals("xx", spc.bucket("xxx"))
  }

  @Test
  def descendsFromPrefixOfLengthOneWithStepOne {
    val spc = StringPrefixCategoryFunction("ss", prefixLength = 1, maxLength = 2, step = 1)
    assertEquals(Some(StringPrefixCategoryFunction("ss", prefixLength = 2, maxLength = 2, step = 1)),
                 spc.descend)
  }

  @Test
  def descendsFromPrefixOfLengthOneWithStepTwo {
    val spc = StringPrefixCategoryFunction("ss", prefixLength = 1, maxLength = 3, step = 2)
    assertEquals(Some(StringPrefixCategoryFunction("ss", prefixLength = 3, maxLength = 3, step = 2)),
                 spc.descend)
  }

  @Test
  def descendsToIndividualWhenPrefixIsSameAsMaxLength {
    val spc = StringPrefixCategoryFunction("ss", prefixLength = 2, maxLength = 2, step = 1)
    assertEquals(None, spc.descend)
  }

  @Test
  def descendsToMaxLengthWhenStepAboutToExceed {
    val spc = StringPrefixCategoryFunction("ss", prefixLength = 1, maxLength = 2, step = 2)
    assertEquals(Some(StringPrefixCategoryFunction("ss", prefixLength = 2, maxLength = 2, step = 2)),
                 spc.descend)
  }

  @Test
  def generatesConstraintForPartition {
    val spc = StringPrefixCategoryFunction("foo", prefixLength = 2, maxLength = 2, step = 1)
    assertEquals(new StringPrefixConstraint("foo", "xx"), spc.constrain("xx"))
  }

  @Test
  def cannotConstrainWhenPartitionIsTooShort {
    val spc = StringPrefixCategoryFunction("foo", prefixLength = 2, maxLength = 2, step = 1)
    assertEquals(new SetConstraint("foo", Set("x")), spc.constrain("x"))
  }

  @Test(expected = classOf[InvalidAttributeValueException])
  def cannotConstrainWhenPartitionIsTooLong {
    val spc = StringPrefixCategoryFunction("ss", prefixLength = 2, maxLength = 2, step = 1)
    spc.constrain("xxx")
  }

  @Test
  def shouldBucketIsAlwaysTrue {
    assertTrue(StringPrefixCategoryFunction("ss", 1, 1, 1).shouldBucket)
  }

  @Test
  def nameShouldBeQuestionMarksFollowedByStar {
    assertEquals("prefix(2,1,1)", StringPrefixCategoryFunction("ss", 2, 1, 1).name)
  }
}