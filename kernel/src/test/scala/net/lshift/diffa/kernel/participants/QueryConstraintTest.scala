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

package net.lshift.diffa.kernel.participants

import org.junit.Test
import org.junit.Assert._


class QueryConstraintTest {

  @Test
  def shouldQueryForEntities = {

    val constaint = ListQueryConstraint("foo", FirstCategoryFunction("abc", "def"), Seq())
    val queryAction = constaint.nextQueryAction("bar", false).get

    assertEquals(
      EntityQueryAction(ListQueryConstraint("foo", IndividualCategoryFunction(), Seq("abc", "def"))),
      queryAction)
  }

  @Test
  def shouldQueryForAggregates = {

    val constaint = ListQueryConstraint("foo", SecondCategoryFunction("abc", "def"), Seq())
    val queryAction1 = constaint.nextQueryAction("bar", false).get

    assertEquals(
      AggregateQueryAction(ListQueryConstraint("foo", FirstCategoryFunction("abc", "def"), Seq("abc", "def"))),
      queryAction1)
  }

  @Test
  def shouldQueryForEntitiesWhenEmpty = {

    val constaint = ListQueryConstraint("foo", SecondCategoryFunction("abc", "def"), Seq())
    val queryAction1 = constaint.nextQueryAction("bar", true).get

    assertEquals(
      EntityQueryAction(ListQueryConstraint("foo", IndividualCategoryFunction(), Seq("abc", "def"))),
      queryAction1)
  }
}

case class FirstCategoryFunction(lower:String,upper:String) extends CategoryFunction {
  def owningPartition(value:String) = null
  def shouldBucket() = false
  def descend(partition:String) = Some(IntermediateResult(lower, upper, IndividualCategoryFunction()))
}

case class SecondCategoryFunction(lower:String,upper:String) extends CategoryFunction {
  def owningPartition(value:String) = null
  def shouldBucket() = false
  def descend(partition:String) = Some(IntermediateResult(lower, upper, FirstCategoryFunction(lower,upper)))
}