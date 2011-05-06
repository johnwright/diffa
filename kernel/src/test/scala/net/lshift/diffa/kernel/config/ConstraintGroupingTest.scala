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

package net.lshift.diffa.kernel.config

import net.lshift.diffa.kernel.config.ConstraintGroupingTest.GroupExpectation
import org.junit.experimental.theories.{Theories, Theory, DataPoint}
import org.junit.runner.RunWith
import org.junit.Assert._
import net.lshift.diffa.kernel.participants.EasyConstraints._
import scala.collection.JavaConversions._
import net.lshift.diffa.kernel.participants.{SetQueryConstraint, QueryConstraint}

@RunWith(classOf[Theories])
class ConstraintGroupingTest {

  @Theory
  def shouldGroupConstraintsForEndpoint(expectation:GroupExpectation) = {
    val endpoint = new Endpoint{categories = expectation.categories}
    assertEquals(expectation.grouped, endpoint.groupedConstraints)
  }
}

object ConstraintGroupingTest {
  case class GroupExpectation(categories:Map[String,CategoryDescriptor], grouped:Seq[Seq[QueryConstraint]])

  @DataPoint def rangeOnly =
    GroupExpectation(
      categories = Map(
        "bizDate" -> new RangeCategoryDescriptor("datetime"),
        "someInt" -> new RangeCategoryDescriptor("int")
      ),
      grouped = Seq(
        Seq(
          unconstrainedDateTime("bizDate"),
          unconstrainedDateTime("someInt")
        )
      )
    )

  @DataPoint def setOnly =
    GroupExpectation(
      categories = Map(
        "someString" -> new SetCategoryDescriptor(Set("A","B"))
      ),
      grouped = Seq(
        Seq(SetQueryConstraint("someString", Set("A"))),
        Seq(SetQueryConstraint("someString", Set("B")))
      )
    )

  @DataPoint def multipleSets =
    GroupExpectation(
      categories = Map(
        "someString" -> new SetCategoryDescriptor(Set("A","B")),
        "someOtherString" -> new SetCategoryDescriptor(Set("1","2"))
      ),
      grouped = Seq(
        Seq(SetQueryConstraint("someString", Set("A")),SetQueryConstraint("someOtherString", Set("1"))),
        Seq(SetQueryConstraint("someString", Set("A")),SetQueryConstraint("someOtherString", Set("2"))),
        Seq(SetQueryConstraint("someString", Set("B")),SetQueryConstraint("someOtherString", Set("1"))),
        Seq(SetQueryConstraint("someString", Set("B")),SetQueryConstraint("someOtherString", Set("2")))
      )
    )

  @DataPoint def singleRangeAndSet =
    GroupExpectation(
      categories = Map(
        "bizDate" -> new RangeCategoryDescriptor("datetime"),
        "someString" -> new SetCategoryDescriptor(Set("A","B"))
      ),
      grouped = Seq(
        Seq(unconstrainedDateTime("bizDate"), SetQueryConstraint("someString", Set("A"))),
        Seq(unconstrainedDateTime("bizDate"), SetQueryConstraint("someString", Set("B")))
      )
    )

  @DataPoint def multipleRangesAndSets =
    GroupExpectation(
      categories = Map(
        "bizDate" -> new RangeCategoryDescriptor("datetime"),
        "someInt" -> new RangeCategoryDescriptor("int"),
        "someString" -> new SetCategoryDescriptor(Set("A","B")),
        "someOtherString" -> new SetCategoryDescriptor(Set("1","2"))
      ),
      grouped = Seq(
        Seq(unconstrainedDateTime("bizDate"),
            unconstrainedDateTime("someInt"),
            SetQueryConstraint("someString", Set("A")),
            SetQueryConstraint("someOtherString", Set("1"))
        ),
        Seq(unconstrainedDateTime("bizDate"),
            unconstrainedDateTime("someInt"),
            SetQueryConstraint("someString", Set("A")),
            SetQueryConstraint("someOtherString", Set("2"))
        ),
        Seq(unconstrainedDateTime("bizDate"),
            unconstrainedDateTime("someInt"),
            SetQueryConstraint("someString", Set("B")),
            SetQueryConstraint("someOtherString", Set("1"))
        ),
        Seq(unconstrainedDateTime("bizDate"),
            unconstrainedDateTime("someInt"),
            SetQueryConstraint("someString", Set("B")),
            SetQueryConstraint("someOtherString", Set("2"))
        )
      )
    )
}