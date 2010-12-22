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
import scala.collection.Map
import scala.collection.JavaConversions._
import net.lshift.diffa.kernel.frontend._
import net.lshift.diffa.kernel.frontend.WireDigest._

class JSONEncodingUtilsTest {

  def time() = new DateTime().toString()

  @Test
  def aggregateDigestRoundTrip() = {
    val d1 = WireDigest(Map("lastUpdated" -> time(), "digest" -> "d1"), Seq("foo","bar"))
    val d2 = WireDigest(Map("lastUpdated" -> time(), "digest" -> "d2"), Seq("baz","who"))
    digestRoundTrip(d1,d2)
  }

  @Test
  def entityVersionRoundTrip() = {
    val v1 = WireDigest(Map("lastUpdated" -> time(), "digest" -> "d1", "id" -> "id1"), Seq("foo","bar"))
    val v2 = WireDigest(Map("lastUpdated" -> time(), "digest" -> "d2", "id" -> "id2"), Seq("baz","who"))
    digestRoundTrip(v1,v2)
  }

  @Test
  def queryConstraintRoundTrip = {

    val constraint1 = new WireConstraint("foo", Map("upper" -> "abc",
                                                    "lower" -> "def",
                                                    "function" -> "xyz"), Seq("a","b","c"))

    val constraint2 = new WireConstraint("bar", Map("upper" -> "qed",
                                                    "lower" -> "fud",
                                                    "function" -> "yes"), Seq("x","y"))

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

  @Test
  def wireEventRoundTrip = {
    val event = WireEvent("baz", Map("foo" -> "bar"), List("a", "b", "c"))
    val serialized = JSONEncodingUtils.serializeEvent(event)
    val deserialized = JSONEncodingUtils.deserializeEvent(serialized)
    assertNotNull(deserialized)
    assertEquals(event, deserialized)
  }

  @Test
  def contentRoundTrip = {
    val content = "foobar"
    val serialized = JSONEncodingUtils.serializeEntityContent(content)
    val deserialized = JSONEncodingUtils.deserializeEntityContent(serialized)
    assertNotNull(deserialized)
    assertEquals(content, deserialized)
  }

  @Test
  def idRequestRoundTrip = {
    val id = "foobar"
    val serialized = JSONEncodingUtils.serializeEntityContentRequest(id)
    val deserialized = JSONEncodingUtils.deserializeEntityContentRequest(serialized)
    assertNotNull(deserialized)
    assertEquals(id, deserialized)
  }

  @Test
  def bodyRequestRoundTrip = {
    val id = "foobar"
    val serialized = JSONEncodingUtils.serializeEntityBodyRequest(id)
    val deserialized = JSONEncodingUtils.deserializeEntityBodyRequest(serialized)
    assertNotNull(deserialized)
    assertEquals(id, deserialized)
  }

  @Test
  def wireResponseRoundTrip = {
    val response = WireResponse("foobar", "up", "down", List("a", "b", "c"))
    val serialized = JSONEncodingUtils.serializeWireResponse(response)
    val deserialized = JSONEncodingUtils.deserializeWireResponse(serialized)
    assertNotNull(deserialized)
    assertEquals(response, deserialized)
  }

  @Test
  def actionRequestRoundTrip = {
    val response = ActionInvocation("foo","bar")
    val serialized = JSONEncodingUtils.serializeActionRequest(response)
    val deserialized = JSONEncodingUtils.deserializeActionRequest(serialized)
    assertNotNull(deserialized)
    assertEquals(response, deserialized)
  }

  @Test
  def actionResponseRoundTrip = {
    val response = InvocationResult("foo","bar")
    val serialized = JSONEncodingUtils.serializeActionResult(response)
    val deserialized = JSONEncodingUtils.deserializeActionResult(serialized)
    assertNotNull(deserialized)
    assertEquals(response, deserialized)
  }

  def digestRoundTrip(d1:WireDigest, d2:WireDigest) = {
    val serialized = JSONEncodingUtils.serializeDigests(Seq(d1,d2))
    val deserialized = JSONEncodingUtils.deserializeDigests(serialized)
    assertNotNull(deserialized)
    assertEquals(2, deserialized.length)
    (Seq(d1,d2), deserialized).zip.foreach(x=> compareDigests(x._1,x._2))
  }

  def compareDigests(expected:WireDigest, actual:WireDigest) = {
    assertEquals(expected.metadata(LAST_UPDATED), actual.metadata(LAST_UPDATED))
    assertEquals(expected.metadata(DIGEST), actual.metadata(DIGEST))
    (expected.attributes, actual.attributes).zip.foreach(x=> assertEquals(x._1,x._2))
    if (expected.metadata.containsKey(ID)) {
      assertEquals(expected.metadata(ID), actual.metadata(ID))
    }
  }

}