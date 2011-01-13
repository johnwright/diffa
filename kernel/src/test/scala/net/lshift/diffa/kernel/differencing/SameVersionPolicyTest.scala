/**
 * Copyright (C) 2011 LShift Ltd.
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

import org.junit.Test
import org.easymock.EasyMock._
import org.joda.time.DateTime
import net.lshift.diffa.kernel.util.DateUtils._
import net.lshift.diffa.kernel.participants._
import java.lang.String
import net.lshift.diffa.kernel.events.VersionID
import net.lshift.diffa.kernel.util.Dates._
import org.easymock.EasyMock
import org.apache.commons.codec.digest.DigestUtils
import net.lshift.diffa.kernel.participants.EasyConstraints._

/**
 * Test cases for the same version policy.
 */
class SameVersionPolicyTest extends AbstractPolicyTest {
  val policy = new SameVersionPolicy(store, listener, configStore)

  def downstreamVersionFor(v:String) = v

  @Test
  def shouldUpdateDownstreamVersionsWhenStoreIsOutOfDateWithDownstreamParticipant {
    val timestamp = new DateTime()
    // Expect only a top-level sync for the upstream, but a full sync for the downstream
    expectUpstreamAggregateSync(Seq(unconstrainedDate(YearlyCategoryFunction)),
      DigestsFromParticipant(
        AggregateDigest(Seq("2009"), START_2009, DigestUtils.md5Hex("vsn1")),
        AggregateDigest(Seq("2010"), START_2010, DigestUtils.md5Hex("vsn2"))),
      VersionsFromStore(
        Up("id1", JUN_6_2009_1, "vsn1"),
        Up("id2", JUL_8_2010_1, "vsn2")))

    expectDownstreamAggregateSync(List(unconstrainedDate(YearlyCategoryFunction)),
      DigestsFromParticipant(
        AggregateDigest(Seq("2009"), START_2009, DigestUtils.md5Hex(downstreamVersionFor("vsn1"))),
        AggregateDigest(Seq("2010"), START_2010, DigestUtils.md5Hex(downstreamVersionFor("vsn2") + downstreamVersionFor("vsn3")))),
      VersionsFromStore(
        Down("id1", JUN_6_2009_1, "vsn1", downstreamVersionFor("vsn1")),
        Down("id2", JUL_8_2010_1, "vsn2", downstreamVersionFor("vsn2")),
        Down("id4", JUL_8_2010_1, "vsn4", downstreamVersionFor("vsn4"))))
    expectDownstreamAggregateSync(List(dateRangeConstraint(START_2010, END_2010, monthly)),
      DigestsFromParticipant(
        AggregateDigest(Seq("2010-07"), JUL_8_2010_1, DigestUtils.md5Hex(downstreamVersionFor("vsn2") + downstreamVersionFor("vsn3")))),
      VersionsFromStore(
        Down("id2", JUL_8_2010_1, "vsn2", downstreamVersionFor("vsn2")),
        Down("id4", JUL_8_2010_1, "vsn4", downstreamVersionFor("vsn4"))))
    expectDownstreamAggregateSync(List(dateRangeConstraint(JUL_2010, END_JUL_2010, daily)),
      DigestsFromParticipant(
        AggregateDigest(Seq("2010-07-08"), JUL_8_2010_1, DigestUtils.md5Hex(downstreamVersionFor("vsn2") + downstreamVersionFor("vsn3")))),
      VersionsFromStore(
        Down("id2", JUL_8_2010_1, "vsn2", downstreamVersionFor("vsn2")),
        Down("id4", JUL_8_2010_1, "vsn4", downstreamVersionFor("vsn4"))))
    expectDownstreamEntitySync2(abPair, List(dateRangeConstraint(JUL_8_2010, endOfDay(JUL_8_2010), individual)),
      DigestsFromParticipant(
        EntityVersion("id2", Seq(JUL_8_2010_1.toString), JUL_8_2010_1, downstreamVersionFor("vsn2")),
        EntityVersion("id3", Seq(JUL_8_2010_1.toString), JUL_8_2010_1, downstreamVersionFor("vsn3"))),
      VersionsFromStore(
        Down("id2", JUL_8_2010_1, "vsn2", downstreamVersionFor("vsn2")),
        Down("id4", JUL_8_2010_1, "vsn4", downstreamVersionFor("vsn4"))))

    // We should see id3 be updated, and id4 be removed
    expect(store.storeDownstreamVersion(VersionID(abPair, "id3"), bizDateMap(JUL_8_2010_1), JUL_8_2010_1, "vsn3", downstreamVersionFor("vsn3"))).
      andReturn(Correlation(null, abPair, "id3", null, bizDateMap(JUL_8_2010_1),JUL_8_2010_1, timestamp, "vsn3", "vsn3", downstreamVersionFor("vsn3"), false))
    expect(store.clearDownstreamVersion(VersionID(abPair, "id4"))).
      andReturn(Correlation.asDeleted(abPair, "id4", new DateTime))

    // We should still see an unmatched version check
    expect(store.unmatchedVersions(EasyMock.eq(abPair), EasyMock.eq(Seq(unconstrainedDate(YearlyCategoryFunction))))).
        andReturn(Seq())
    replayAll

    policy.difference(abPair, usMock, dsMock, nullListener)
    verifyAll
  }
}