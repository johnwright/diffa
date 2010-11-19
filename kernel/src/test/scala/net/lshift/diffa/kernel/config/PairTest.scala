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

package net.lshift.diffa.kernel.config

import org.junit.Test
import org.junit.Assert._
import net.lshift.diffa.kernel.participants.YearlyCategoryFunction
import net.lshift.diffa.kernel.participants.EasyConstraints._
import scala.collection.Map
import net.lshift.diffa.kernel.util.Conversions._

class PairTest {

  @Test
  def defaultConstraints() = {
    var pair = new Pair()
    assertEquals(Seq(unconstrainedDate(YearlyCategoryFunction)), pair.defaultConstraints)
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

    var pair = new Pair{categories = categoryMap}
    assertEquals(schematized, pair.schematize(rightOrder))
  }


}
