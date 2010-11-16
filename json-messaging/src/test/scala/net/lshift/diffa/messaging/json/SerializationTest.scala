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

package net.lshift.diffa.messaging.json

import org.junit.Test
import org.junit.Assert._
import net.lshift.diffa.kernel.participants.EasyConstraints._
import org.joda.time.DateTime
import net.lshift.diffa.kernel.participants.{YearlyCategoryFunction, IndividualCategoryFunction}

class SerializationTest {

  @Test
  def roundTrip = {
    val start1 = new DateTime()
    val end1 = new DateTime()
    val function1 = IndividualCategoryFunction()
    val start2 = new DateTime()
    val end2 = new DateTime()
    val function2 = YearlyCategoryFunction()
    val constraint1 = dateRangeConstaint(start1, end1, function1)
    val constraint2 = dateRangeConstaint(start2, end2, function2)
    val serialized = JSONEncodingUtils.serialize(Seq(constraint1, constraint2))
    val deserialized = JSONEncodingUtils.deserialize(serialized)
    assertNotNull(deserialized)
    assertEquals(2, deserialized.length)
    assertEquals(constraint1, deserialized(0))
    assertEquals(constraint2, deserialized(1))

  }
}