package net.lshift.diffa.kernel.frontend.wire

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

import org.junit.Test
import org.junit.Assert._
import org.joda.time.DateTime
import WireConstraint._
import net.lshift.diffa.kernel.participants._

class WireConstraintTest {

  @Test
  def rangeToWireAndBack = {
    val start = new DateTime
    val end = new DateTime
    val constraint = RangeQueryConstraint("date", YearlyCategoryFunction, Seq(start.toString(), end.toString()))
    val expectation = rangeConstraint("date", YearlyCategoryFunction, start,end)
    roundTrip(expectation, constraint)
  }

  @Test
  def listToWireAndBack = {
    val list = Seq("1","2","3")
    val constraint = ListQueryConstraint("date", YearlyCategoryFunction, list)
    val expectation = listConstraint("date", YearlyCategoryFunction, list)
    roundTrip(expectation, constraint)
  }

  @Test
  def unboundedToWireAndBack = {
    val constraint = UnboundedRangeQueryConstraint("date", YearlyCategoryFunction)
    val expectation = unbounded("date", YearlyCategoryFunction)
    roundTrip(expectation, constraint)
  }

  def roundTrip(expectation:WireConstraint, input:QueryConstraint) = {
    val wire = input.wireFormat
    assertEquals(expectation, wire)
    val resolved = ConstraintRegistry.resolve(wire)
    assertEquals(input, resolved)
  }
}