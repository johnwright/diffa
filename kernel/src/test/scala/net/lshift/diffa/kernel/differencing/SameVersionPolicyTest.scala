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

package net.lshift.diffa.kernel.differencing

import java.lang.String

import org.junit.Test
import org.easymock.EasyMock._
import org.joda.time.DateTime
import org.easymock.EasyMock
import org.apache.commons.codec.digest.DigestUtils

import net.lshift.diffa.kernel.participants._
import net.lshift.diffa.kernel.events.VersionID
import net.lshift.diffa.kernel.util.FullDateTimes._
import scala.collection.JavaConversions._


/**
 * Test cases for the same version policy.
 */
class SameVersionPolicyTest extends AbstractPolicyTest {
  val policy = new SameVersionPolicy(stores, listener, configStore)

  def downstreamVersionFor(v:String) = v

  @Test
  def shouldUpdateDownstreamVersionsWhenStoreIsOutOfDateWithDownstreamParticipantForDateCategories =
    shouldUpdateDownstreamVersionsWhenStoreIsOutOfDateWithDownstreamParticipant(dateCategoryData)

  @Test
  def shouldUpdateDownstreamVersionsWhenStoreIsOutOfDateWithDownstreamParticipantForIntegerCategories =
    shouldUpdateDownstreamVersionsWhenStoreIsOutOfDateWithDownstreamParticipant(integerCategoryData)

  protected def shouldUpdateDownstreamVersionsWhenStoreIsOutOfDateWithDownstreamParticipant(testData: PolicyTestData) {
    pair.upstream.categories = testData.upstreamCategories
    pair.downstream.categories = testData.downstreamCategories
    val timestamp = new DateTime
    // Expect only a top-level sync for the upstream, but a full sync for the downstream
    expectUpstreamAggregateSync(testData.bucketing(0), testData.constraints(0),
      DigestsFromParticipant(
        AggregateDigest(testData.attributes(0), DigestUtils.md5Hex("vsn1")),
        AggregateDigest(testData.attributes(1), DigestUtils.md5Hex("vsn2"))),
      VersionsFromStore(
        Up("id1", testData.valueKey, testData.values(0), "vsn1"),
        Up("id2", testData.valueKey, testData.values(1), "vsn2")))

    expectDownstreamAggregateSync(testData.bucketing(0), testData.constraints(0),
      DigestsFromParticipant(
        AggregateDigest(testData.attributes(0), DigestUtils.md5Hex(downstreamVersionFor("vsn1"))),
        AggregateDigest(testData.attributes(1), DigestUtils.md5Hex(downstreamVersionFor("vsn2") + downstreamVersionFor("vsn3")))),
      VersionsFromStore(
        Down("id1", testData.valueKey, testData.values(0), "vsn1", downstreamVersionFor("vsn1")),
        Down("id2", testData.valueKey, testData.values(1), "vsn2", downstreamVersionFor("vsn2")),
        Down("id4", testData.valueKey, testData.values(1), "vsn4", downstreamVersionFor("vsn4"))))
    expectDownstreamAggregateSync(testData.bucketing(1), testData.constraints(1),
      DigestsFromParticipant(
        AggregateDigest(testData.attributes(2), DigestUtils.md5Hex(downstreamVersionFor("vsn2") + downstreamVersionFor("vsn3")))),
      VersionsFromStore(
        Down("id2", testData.valueKey, testData.values(1), "vsn2", downstreamVersionFor("vsn2")),
        Down("id4", testData.valueKey, testData.values(1), "vsn4", downstreamVersionFor("vsn4"))))
    expectDownstreamAggregateSync(testData.bucketing(2), testData.constraints(2),
      DigestsFromParticipant(
        AggregateDigest(testData.attributes(3), DigestUtils.md5Hex(downstreamVersionFor("vsn2") + downstreamVersionFor("vsn3")))),
      VersionsFromStore(
        Down("id2", testData.valueKey, testData.values(1), "vsn2", downstreamVersionFor("vsn2")),
        Down("id4", testData.valueKey, testData.values(1), "vsn4", downstreamVersionFor("vsn4"))))
    expectDownstreamEntitySync2(abPair, testData.constraints(3),
      DigestsFromParticipant(
        EntityVersion("id2", Seq(testData.values(1).toString), JUL_8_2010_1, downstreamVersionFor("vsn2")),
        EntityVersion("id3", Seq(testData.values(1)toString), JUL_8_2010_1, downstreamVersionFor("vsn3"))),
      VersionsFromStore(
        Down("id2", testData.valueKey, testData.values(1), "vsn2", downstreamVersionFor("vsn2")),
        Down("id4", testData.valueKey, testData.values(1), "vsn4", downstreamVersionFor("vsn4"))))

    // We should see id3 be updated, and id4 be removed
    expect(writer.storeDownstreamVersion(VersionID(abPair, "id3"), testData.downstreamAttributes(1), JUL_8_2010_1, "vsn3", downstreamVersionFor("vsn3"))).
      andReturn(Correlation(null, abPair, "id3", null, toStrMap(testData.downstreamAttributes(1)), JUL_8_2010_1, timestamp, "vsn3", "vsn3", downstreamVersionFor("vsn3"), false))
    expect(writer.clearDownstreamVersion(VersionID(abPair, "id4"))).
      andReturn(Correlation.asDeleted(abPair, "id4", new DateTime))

    // We should see events indicating that id4 to enter a matched state (since the deletion made the sides line up)
    listener.onMatch(VersionID(abPair, "id4"), null, LiveWindow); expectLastCall

    // We should still see an unmatched version check
    expect(stores(pair.key).unmatchedVersions(EasyMock.eq(testData.constraints(0)), EasyMock.eq(testData.constraints(0)))).andReturn(Seq()).times(2)
    replayAll

    policy.scanUpstream(abPair, writer, usMock, nullListener, feedbackHandle)
    policy.scanDownstream(abPair, writer, usMock, dsMock, listener, feedbackHandle)
    policy.replayUnmatchedDifferences(abPair, listener)

    verifyAll
  }
}