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

/**
 * Test cases for the Endpoint class.
 */
class EndpointTest {

  val dateCategoryDescriptor = new RangeCategoryDescriptor("date")
  val intCategoryDescriptor = new RangeCategoryDescriptor("int")

  @Test
  def defaultConstraintsForEndpointWithNoCategories = {
    val ep = new Endpoint()
    assertEquals(Seq(), ep.defaultConstraints)
  }

    @Test
  def defaultConstraintsForEndpointWithDateCategory = {
    val ep = new Endpoint(categories=Map("bizDate" -> dateCategoryDescriptor))
    assertEquals(Seq(unconstrainedDate("bizDate")), ep.defaultConstraints)
  }

  @Test
  def defaultConstraintsForEndpointWithIntCategory = {
    val ep = new Endpoint(categories=Map("someInt" -> intCategoryDescriptor))
    assertEquals(Seq(unconstrainedInt("someInt")), ep.defaultConstraints)
  }

  @Test
  def schematize() = {
    val categoryMap = Map("xyz_attribute" -> intCategoryDescriptor,
                          "abc_attribute" -> dateCategoryDescriptor,
                          "def_attribute" -> dateCategoryDescriptor)

    val rightOrder = Seq("2011-01-26T10:24:00.000Z" /* abc */ ,"2011-01-26T10:36:00.000Z" /* def */, "55" /* xyz */)

    val schematized = Map("xyz_attribute" -> IntegerAttribute(55),
                          "abc_attribute" -> DateAttribute(new DateTime(2011, 1, 26, 10, 24, 0, 0)),    // TODO: Specify timezone
                          "def_attribute" -> DateAttribute(new DateTime(2011, 1, 26, 10, 36, 0, 0)))    // TODO: Specify timezone

    var ep = new Endpoint{categories = categoryMap}
    assertEquals(schematized, ep.schematize(rightOrder))
  }
}