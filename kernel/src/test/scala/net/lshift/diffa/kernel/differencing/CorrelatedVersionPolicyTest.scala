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

import org.easymock.EasyMock._
import org.junit.Test
import org.joda.time.DateTime
import org.easymock.EasyMock
import org.apache.commons.codec.digest.DigestUtils

import net.lshift.diffa.kernel.participants._
import net.lshift.diffa.kernel.events.VersionID
import net.lshift.diffa.kernel.util.FullDateTimes._

import scala.collection.JavaConversions._

/**
 * Test cases for the correlated version policy test.
 */
class CorrelatedVersionPolicyTest extends AbstractPolicyTest {
  val policy = new CorrelatedVersionPolicy(stores, listener, configStore)

  /**
   * Generates the internal downstream version of a given version string. Since the correlated policy expects
   * differing versions, this method will append a suffix to the version.
   */
  protected def downstreamVersionFor(v:String) = v + "-dvsn"

  @Test
  def shouldUpdateDownstreamVersionsWhenStoreIsOutOfDateWithDownstreamParticipantForDateCategories =
    shouldUpdateDownstreamVersionsWhenStoreIsOutOfDateWithDownstreamParticipant(dateCategoryData)

  @Test
  def shouldUpdateDownstreamVersionsWhenStoreIsOutOfDateWithDownstreamParticipantForIntegerCategories =
    shouldUpdateDownstreamVersionsWhenStoreIsOutOfDateWithDownstreamParticipant(integerCategoryData)

  @Test
  def shouldGenerateADifferenceWhenDownstreamResyncFailsForDateCategories =
    shouldGenerateADifferenceWhenDownstreamResyncFails(dateCategoryData)

  @Test
  def shouldGenerateADifferenceWhenDownstreamResyncFailsForIntegerCategories =
    shouldGenerateADifferenceWhenDownstreamResyncFails(integerCategoryData)

  protected def shouldUpdateDownstreamVersionsWhenStoreIsOutOfDateWithDownstreamParticipant(
      testData: PolicyTestData) {
    pair.upstream.categories = testData.upstreamCategories
    pair.downstream.categories = testData.downstreamCategories
    val timestamp = new DateTime
    // Expect only a top-level sync for the upstream, but a full sync for the downstream
    expectUpstreamAggregateSync(abPair, testData.bucketing(0), testData.constraints(0),
      DigestsFromParticipant(
        AggregateDigest(testData.attributes(0), DigestUtils.md5Hex("vsn1")),
        AggregateDigest(testData.attributes(1), DigestUtils.md5Hex("vsn2"))),
      VersionsFromStore(
        Up("id1", testData.valueKey, testData.values(0), "vsn1"),
        Up("id2", testData.valueKey, testData.values(1), "vsn2")))

    expectDownstreamAggregateSync(abPair, testData.bucketing(0), testData.constraints(0),
      DigestsFromParticipant(
        AggregateDigest(testData.attributes(0), DigestUtils.md5Hex(downstreamVersionFor("vsn1"))),
        AggregateDigest(testData.attributes(1), DigestUtils.md5Hex(downstreamVersionFor("vsn2") + downstreamVersionFor("vsn3") + downstreamVersionFor("vsn5a")))),
      VersionsFromStore(
        Down("id1", testData.valueKey, testData.values(0), "vsn1", downstreamVersionFor("vsn1")),
        Down("id2", testData.valueKey, testData.values(1), "vsn2", downstreamVersionFor("vsn2")),
        Down("id4", testData.valueKey, testData.values(1), "vsn4", downstreamVersionFor("vsn4")),
        Down("id5", testData.valueKey, testData.values(1), "vsn5", downstreamVersionFor("vsn5"))))
    expectDownstreamAggregateSync(abPair, testData.bucketing(1), testData.constraints(1),
      DigestsFromParticipant(
        AggregateDigest(testData.attributes(2), DigestUtils.md5Hex(downstreamVersionFor("vsn2") + downstreamVersionFor("vsn3") + downstreamVersionFor("vsn5a")))),
      VersionsFromStore(
        Down("id2", testData.valueKey, testData.values(1), "vsn2", downstreamVersionFor("vsn2")),
        Down("id4", testData.valueKey, testData.values(1), "vsn4", downstreamVersionFor("vsn4")),
        Down("id5", testData.valueKey, testData.values(1), "vsn5", downstreamVersionFor("vsn5"))))
    expectDownstreamAggregateSync(abPair, testData.bucketing(2), testData.constraints(2),
      DigestsFromParticipant(
        AggregateDigest(testData.attributes(3), DigestUtils.md5Hex(downstreamVersionFor("vsn2") + downstreamVersionFor("vsn3") + downstreamVersionFor("vsn5a")))),
      VersionsFromStore(
        Down("id2", testData.valueKey, testData.values(1), "vsn2", downstreamVersionFor("vsn2")),
        Down("id4", testData.valueKey, testData.values(1), "vsn4", downstreamVersionFor("vsn4")),
        Down("id5", testData.valueKey, testData.values(1), "vsn5", downstreamVersionFor("vsn5"))))
    expectDownstreamEntitySync2(abPair, testData.constraints(3),
      DigestsFromParticipant(
        EntityVersion("id2", Seq(testData.values(1).toString), JUL_8_2010_1, downstreamVersionFor("vsn2")),
        EntityVersion("id3", Seq(testData.values(1).toString), JUL_8_2010_1, downstreamVersionFor("vsn3")),
        EntityVersion("id5", Seq(testData.values(1).toString), JUL_8_2010_1, downstreamVersionFor("vsn5a"))),
      VersionsFromStore(
        Down("id2", testData.valueKey, testData.values(1), "vsn2", downstreamVersionFor("vsn2")),
        Down("id4", testData.valueKey, testData.values(1), "vsn4", downstreamVersionFor("vsn4")),
        Down("id5", testData.valueKey, testData.values(1), "vsn5", downstreamVersionFor("vsn5"))))

    // We should see id3 re-run through the system, and id4 be removed
    expect(usMock.retrieveContent("id3")).andReturn("content3")
    expect(dsMock.generateVersion("content3")).andReturn(ProcessingResponse("id3", Seq(testData.values(1).toString), "vsn3", downstreamVersionFor("vsn3")))
    expect(writer.storeDownstreamVersion(VersionID(abPair, "id3"), testData.downstreamAttributes(1), JUL_8_2010_1, "vsn3", downstreamVersionFor("vsn3"))).
      andReturn(Correlation(null, abPair, "id3", null, toStrMap(testData.downstreamAttributes(1)), JUL_8_2010_1, timestamp, "vsn3", "vsn3", downstreamVersionFor("vsn3"), false))
    expect(writer.clearDownstreamVersion(VersionID(abPair, "id4"))).
      andReturn(Correlation.asDeleted(abPair, "id4", new DateTime))
    expect(usMock.retrieveContent("id5")).andReturn("content5")
    expect(dsMock.generateVersion("content5")).andReturn(ProcessingResponse("id5", Seq(testData.values(1).toString), "vsn5a", downstreamVersionFor("vsn5a")))
    expect(writer.storeDownstreamVersion(VersionID(abPair, "id5"), testData.downstreamAttributes(1), JUL_8_2010_1, "vsn5a", downstreamVersionFor("vsn5a"))).
        andReturn(Correlation(null, abPair, "id3", null, toStrMap(testData.downstreamAttributes(1)), JUL_8_2010_1, timestamp, "vsn5a", "vsn5a", downstreamVersionFor("vsn5a"), false))

    // We should see events indicating that id4 to enter a matched state (since the deletion made the sides line up)
    listener.onMatch(VersionID(abPair, "id4"), null, LiveWindow); expectLastCall

    // We should still see an unmatched version check
    expect(stores(abPair).unmatchedVersions(EasyMock.eq(testData.constraints(0)), EasyMock.eq(testData.constraints(0)))).andReturn(Seq())
    replayAll

    policy.scanUpstream(abPair, writer, usMock, nullListener, feedbackHandle)
    policy.scanDownstream(abPair, writer, usMock, dsMock, nullListener, feedbackHandle)
    policy.replayUnmatchedDifferences(abPair, nullListener)

    verifyAll
  }

  protected def shouldGenerateADifferenceWhenDownstreamResyncFails(testData: PolicyTestData) {
    pair.upstream.categories = testData.upstreamCategories
    pair.downstream.categories = testData.downstreamCategories
    // Expect only a top-level sync between the pairs
    expectUpstreamAggregateSync(testData.bucketing(0), testData.constraints(0),
      DigestsFromParticipant(
        AggregateDigest(testData.attributes(0), DigestUtils.md5Hex("vsn1")),
        AggregateDigest(testData.attributes(1), DigestUtils.md5Hex("vsn2"))),
      VersionsFromStore(
        Up("id1", testData.valueKey, testData.values(0), "vsn1"),
        Up("id2", testData.valueKey, testData.values(1), "vsn2")))

    expectDownstreamAggregateSync(testData.bucketing(0), testData.constraints(0),
      DigestsFromParticipant(
        AggregateDigest(testData.attributes(1), DigestUtils.md5Hex(downstreamVersionFor("vsn2") + downstreamVersionFor("vsn3")))),
      VersionsFromStore(
        Down("id2", testData.valueKey, testData.values(1), "vsn2", downstreamVersionFor("vsn2"))))
    expectDownstreamAggregateSync(testData.bucketing(1), testData.constraints(1),
      DigestsFromParticipant(
        AggregateDigest(testData.attributes(2), DigestUtils.md5Hex(downstreamVersionFor("vsn2") + downstreamVersionFor("vsn3")))),
      VersionsFromStore(
        Down("id2", testData.valueKey, testData.values(1), "vsn2", downstreamVersionFor("vsn2"))))
    expectDownstreamAggregateSync(testData.bucketing(2), testData.constraints(2),
      DigestsFromParticipant(
        AggregateDigest(testData.attributes(3), DigestUtils.md5Hex(downstreamVersionFor("vsn2") + downstreamVersionFor("vsn3")))),
      VersionsFromStore(
        Down("id2", testData.valueKey, testData.values(1), "vsn2", downstreamVersionFor("vsn2"))))
    expectDownstreamEntitySync2(abPair, testData.constraints(3),
      DigestsFromParticipant(
        EntityVersion("id2", Seq(testData.values(1).toString), JUL_8_2010_1, downstreamVersionFor("vsn2")),
        EntityVersion("id3", Seq(testData.values(1).toString), JUL_8_2010_1, downstreamVersionFor("vsn3"))),
      VersionsFromStore(
        Down("id2", testData.valueKey, testData.values(1), "vsn2", downstreamVersionFor("vsn2"))))

    // We should see id3 re-run through the system, but not be stored since the version on the downstream is different
    expect(usMock.retrieveContent("id3")).andReturn("content3a")
    expect(dsMock.generateVersion("content3a")).andReturn(ProcessingResponse("id3", Seq(testData.values(1).toString), "vsn3a", downstreamVersionFor("vsn3a")))

    // We should see a replayStoredDifferences being generated
    listener.onMismatch(VersionID(abPair, "id3"), JUL_8_2010_1, downstreamVersionFor("vsn3a"), downstreamVersionFor("vsn3"), TriggeredByScan); expectLastCall

    // We should still see an unmatched version check
    expect(stores(abPair).unmatchedVersions(EasyMock.eq(testData.constraints(0)), EasyMock.eq(testData.constraints(0)))).andReturn(Seq())
    replayAll

    policy.scanUpstream(abPair, writer, usMock, listener, feedbackHandle)
    policy.scanDownstream(abPair, writer, usMock, dsMock, listener, feedbackHandle)
    policy.replayUnmatchedDifferences(abPair, nullListener)

    verifyAll
  }
}