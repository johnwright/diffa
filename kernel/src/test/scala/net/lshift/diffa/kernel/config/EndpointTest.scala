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

/**
 * Test cases for the Endpoint class.
 */
class EndpointTest {
  @Test
  def defaultConstraintsForEndpointWithNoCategories = {
    val ep = new Endpoint()
    assertEquals(Seq(), ep.defaultConstraints)
  }

    @Test
  def defaultConstraintsForEndpointWithDateCategory = {
    val ep = new Endpoint(categories=Map("bizDate" -> "date"))
    assertEquals(Seq(unconstrainedDate("bizDate")), ep.defaultConstraints)
  }

  @Test
  def defaultConstraintsForEndpointWithIntCategory = {
    val ep = new Endpoint(categories=Map("someInt" -> "int"))
    assertEquals(Seq(unconstrainedInt("someInt")), ep.defaultConstraints)
  }

  @Test
  def schematize() = {
    val categoryMap = Map("xyz_attribute" -> "type_of_xyz",
                          "abc_attribute" -> "type_of_abc",
                          "def_attribute" -> "type_of_def")

    val rightOrder = Seq("abc_value","def_value", "xyz_value")

    val schematized = Map("xyz_attribute" -> "xyz_value",
                          "abc_attribute" -> "abc_value",
                          "def_attribute" -> "def_value")

    var ep = new Endpoint{categories = categoryMap}
    assertEquals(schematized, ep.schematize(rightOrder))
  }
}