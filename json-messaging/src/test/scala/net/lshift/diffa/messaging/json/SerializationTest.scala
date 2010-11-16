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
import net.lshift.diffa.kernel.participants._

class SerializationTest {

  @Test
  def aggregateDigestRoundTrip() = {
    val d1 = AggregateDigest(Seq("foo","bar"), new DateTime, "digest1")
    val d2 = AggregateDigest(Seq("baz","who"), new DateTime, "digest2")
    val serialized = JSONEncodingUtils.serializeDigests(Seq(d1,d2))
    val deserialized = JSONEncodingUtils.deserializeAggregateDigest(serialized)
    assertNotNull(deserialized)
    assertEquals(2, deserialized.length)
    (Seq(d1,d2), deserialized).zip.foreach(x=> compareDigests(x._1,x._2))
  }

  @Test
  def entityVersionRoundTrip() = {
    val v1 = EntityVersion("id1", Seq("foo","bar"), new DateTime, "digest1")
    val v2 = EntityVersion("id2", Seq("baz","who"), new DateTime, "digest2")
    val serialized = JSONEncodingUtils.serializeDigests(Seq(v1,v2))
    val deserialized = JSONEncodingUtils.deserializeEntityVersions(serialized)
    assertNotNull(deserialized)
    assertEquals(2, deserialized.length)
    (Seq(v1,v2), deserialized).zip.foreach(x=> compareVersions(x._1,x._2))
  }

  @Test
  def queryConstraintRoundTrip = {
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

  @Test
  def emptyList = {
    val serialized = JSONEncodingUtils.serialize(Seq())
    val deserialized = JSONEncodingUtils.deserialize(serialized)
    assertNotNull(deserialized)
    assertEquals(0, deserialized.length)
  }

  def compareDigests(expected:Digest, actual:Digest) = {
    // TODO Date comparison currnently fails because the chronology is wrong
    assertEquals(expected.lastUpdated.getMillis, actual.lastUpdated.getMillis)
    assertEquals(expected.digest, actual.digest)
    (expected.attributes, actual.attributes).zip.foreach(x=> assertEquals(x._1,x._2))
  }

  def compareVersions(expected:EntityVersion, actual:EntityVersion) = {
    assertEquals(expected.id, actual.id)
    compareDigests(expected, actual)
  }
}