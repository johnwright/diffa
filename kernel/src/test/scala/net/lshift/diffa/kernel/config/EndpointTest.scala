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

import org.junit.Test
import org.junit.Assert._
import scala.collection.JavaConversions._
import net.lshift.diffa.kernel.participants.EasyConstraints._
import org.joda.time.DateTime
import net.lshift.diffa.kernel.differencing.{DateAttribute, IntegerAttribute}
import org.junit.runner.RunWith
import org.junit.experimental.theories.{DataPoint, Theories, Theory, DataPoints}
import net.lshift.diffa.kernel.config.EndpointTest.ConstraintExpectation
import net.lshift.diffa.kernel.participants.{IntegerRangeConstraint, DateRangeConstraint, QueryConstraint}

/**
 * Test cases for the Endpoint class.
 */


@RunWith(classOf[Theories])
class EndpointTest {

  @Test
  def defaultConstraintsForEndpointWithNoCategories = {
    val ep = new Endpoint()
    assertEquals(Seq(), ep.defaultConstraints)
  }

  @Theory
  def shouldBuildConstraintsForEndpoint(expectation:ConstraintExpectation) = {
    val ep = new Endpoint(categories=Map(expectation.name -> expectation.descriptor))
    assertEquals(Seq(expectation.constraint), ep.defaultConstraints)
  }

  @Test
  def schematize() = {
    val unboundDateCategoryDescriptor = new RangeCategoryDescriptor("date")
    val unboundIntCategoryDescriptor = new RangeCategoryDescriptor("int")

    val categoryMap = Map("xyz_attribute" -> unboundIntCategoryDescriptor,
                          "abc_attribute" -> unboundDateCategoryDescriptor,
                          "def_attribute" -> unboundDateCategoryDescriptor)

    val rightOrder = Seq("2011-01-26T10:24:00.000Z" /* abc */ ,"2011-01-26T10:36:00.000Z" /* def */, "55" /* xyz */)

    val schematized = Map("xyz_attribute" -> IntegerAttribute(55),
                          "abc_attribute" -> DateAttribute(new DateTime(2011, 1, 26, 10, 24, 0, 0)),    // TODO: Specify timezone
                          "def_attribute" -> DateAttribute(new DateTime(2011, 1, 26, 10, 36, 0, 0)))    // TODO: Specify timezone

    var ep = new Endpoint{categories = categoryMap}
    assertEquals(schematized, ep.schematize(rightOrder))
  }
}

object EndpointTest {

  case class ConstraintExpectation(name:String, descriptor:RangeCategoryDescriptor, constraint:QueryConstraint)

  @DataPoints def unbounded =
    Array(
      ConstraintExpectation("bizDate", new RangeCategoryDescriptor("date"), unconstrainedDate("bizDate")),
      ConstraintExpectation("someInt", new RangeCategoryDescriptor("int"), unconstrainedInt("someInt"))
   )

  @DataPoints def bounded =
    Array(
      ConstraintExpectation("bizDate",
        new RangeCategoryDescriptor("date", "2011-01-01", "2011-01-31"),
        DateRangeConstraint("bizDate", new DateTime(2011,1,1,0,0,0,0), new DateTime(2011,1,31,0,0,0,0))),
      ConstraintExpectation("bizDate",
        new RangeCategoryDescriptor("date", "1998-11-21T00:00:00.000Z", "1998-11-29T00:00:00.000Z"),
        DateRangeConstraint("bizDate", new DateTime(1998,11,21,0,0,0,0), new DateTime(1998,11,29,0,0,0,0))),
      ConstraintExpectation("someInt",
        new RangeCategoryDescriptor("int", "0", "9"),
        IntegerRangeConstraint("someInt", 0, 9))
   )
}
