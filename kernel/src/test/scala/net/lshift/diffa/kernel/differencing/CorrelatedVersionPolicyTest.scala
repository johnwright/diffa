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

package net.lshift.diffa.kernel.differencing

import org.easymock.EasyMock._
import org.junit.Test
import net.lshift.diffa.kernel.util.Dates._
import net.lshift.diffa.kernel.util.DateUtils._
import net.lshift.diffa.kernel.util.EasyMockScalaUtils._
import org.joda.time.DateTime
import org.easymock.{IAnswer, EasyMock}
import org.apache.commons.codec.digest.DigestUtils
import net.lshift.diffa.kernel.participants._
import net.lshift.diffa.kernel.events.{DownstreamCorrelatedChangeEvent, DownstreamChangeEvent, UpstreamChangeEvent, VersionID}

/**
 * Test cases for the correlated version policy test.
 */
class CorrelatedVersionPolicyTest extends AbstractPolicyTest {
  val policy = new CorrelatedVersionPolicy(store, listener)

  /**
   * Generates the internal downstream version of a given version string. Since the correlated policy expects
   * differing versions, this method will append a suffix to the version.
   */
  protected def downstreamVersionFor(v:String) = v + "-dvsn"

  @Test
  def shouldUpdateDownstreamVersionsWhenStoreIsOutOfDateWithDownstreamParticipant {
    val timestamp = new DateTime()
    // Expect only a top-level sync for the upstream, but a full sync for the downstream
    expectUpstreamSync(abPair, List(SimpleDateRangeConstraint(START_2009, END_2010)),
    //expectUpstreamSync(abPair, DateConstraint(START_2009, END_2010), YearGranularity,
      DigestsFromParticipant(
        //VersionDigest("2009", START_2009, START_2009, DigestUtils.md5Hex("vsn1")),
        VersionDigest(Seq("2009"), START_2009, DigestUtils.md5Hex("vsn1")),
        //VersionDigest("2010", START_2010, START_2010, DigestUtils.md5Hex("vsn2"))),
        VersionDigest(Seq("2010"), START_2010, DigestUtils.md5Hex("vsn2"))),
      VersionsFromStore(
        UpstreamVersion(VersionID(abPair, "id1"), categories, JUN_6_2009_1, "vsn1"),
        //UpstreamVersion(VersionID(abPair, "id1"), JUN_6_2009_1, JUN_6_2009_1, "vsn1"),
        UpstreamVersion(VersionID(abPair, "id2"), categories, JUL_8_2010_1, "vsn2")))
        //UpstreamVersion(VersionID(abPair, "id2"), JUL_8_2010_1, JUL_8_2010_1, "vsn2")))

    expectDownstreamSync(abPair, List(SimpleDateRangeConstraint(START_2009, END_2010)),
    //expectDownstreamSync(abPair, DateConstraint(START_2009, END_2010), YearGranularity,
      DigestsFromParticipant(
        //VersionDigest("2009", START_2009, START_2009, DigestUtils.md5Hex(downstreamVersionFor("vsn1"))),
        VersionDigest(Seq("2009"), START_2009, DigestUtils.md5Hex(downstreamVersionFor("vsn1"))),
        //VersionDigest("2010", START_2010, START_2010, DigestUtils.md5Hex(downstreamVersionFor("vsn2") + downstreamVersionFor("vsn3") + downstreamVersionFor("vsn5a")))),
        VersionDigest(Seq("2010"), START_2010, DigestUtils.md5Hex(downstreamVersionFor("vsn2") + downstreamVersionFor("vsn3") + downstreamVersionFor("vsn5a")))),
      VersionsFromStore(
        DownstreamVersion(VersionID(abPair, "id1"), categories, JUN_6_2009_1, "vsn1", downstreamVersionFor("vsn1")),
        //DownstreamVersion(VersionID(abPair, "id1"), JUN_6_2009_1, JUN_6_2009_1, "vsn1", downstreamVersionFor("vsn1")),
        DownstreamVersion(VersionID(abPair, "id2"), categories, JUL_8_2010_1, "vsn2", downstreamVersionFor("vsn2")),
        //DownstreamVersion(VersionID(abPair, "id2"), JUL_8_2010_1, JUL_8_2010_1, "vsn2", downstreamVersionFor("vsn2")),
        DownstreamVersion(VersionID(abPair, "id4"), categories, JUL_8_2010_1, "vsn4", downstreamVersionFor("vsn4")),
        //DownstreamVersion(VersionID(abPair, "id4"), JUL_8_2010_1, JUL_8_2010_1, "vsn4", downstreamVersionFor("vsn4")),
        DownstreamVersion(VersionID(abPair, "id5"), categories, JUL_8_2010_1, "vsn5", downstreamVersionFor("vsn5"))))
        //DownstreamVersion(VersionID(abPair, "id5"), JUL_8_2010_1, JUL_8_2010_1, "vsn5", downstreamVersionFor("vsn5"))))
    expectDownstreamSync(abPair, List(SimpleDateRangeConstraint(START_2010, END_2010)),
    //expectDownstreamSync(abPair, DateConstraint(START_2010, END_2010), MonthGranularity,
      DigestsFromParticipant(
        //VersionDigest("2010-07", JUL_8_2010_1, JUL_8_2010_1, DigestUtils.md5Hex(downstreamVersionFor("vsn2") + downstreamVersionFor("vsn3") + downstreamVersionFor("vsn5a")))),
        VersionDigest(Seq("2010-07"), JUL_8_2010_1, DigestUtils.md5Hex(downstreamVersionFor("vsn2") + downstreamVersionFor("vsn3") + downstreamVersionFor("vsn5a")))),
      VersionsFromStore(
        DownstreamVersion(VersionID(abPair, "id2"), categories, JUL_8_2010_1, "vsn2", downstreamVersionFor("vsn2")),
        //DownstreamVersion(VersionID(abPair, "id2"), JUL_8_2010_1, JUL_8_2010_1, "vsn2", downstreamVersionFor("vsn2")),
        DownstreamVersion(VersionID(abPair, "id4"), categories, JUL_8_2010_1, "vsn4", downstreamVersionFor("vsn4")),
        //DownstreamVersion(VersionID(abPair, "id4"), JUL_8_2010_1, JUL_8_2010_1, "vsn4", downstreamVersionFor("vsn4")),
        DownstreamVersion(VersionID(abPair, "id5"), categories, JUL_8_2010_1, "vsn5", downstreamVersionFor("vsn5"))))
        //DownstreamVersion(VersionID(abPair, "id5"), JUL_8_2010_1, JUL_8_2010_1, "vsn5", downstreamVersionFor("vsn5"))))
    expectDownstreamSync(abPair, List(SimpleDateRangeConstraint(JUL_2010, END_JUL_2010)),
    //expectDownstreamSync(abPair, DateConstraint(JUL_2010, END_JUL_2010), DayGranularity,
      DigestsFromParticipant(
        //VersionDigest("2010-07-08", JUL_8_2010_1, JUL_8_2010_1, DigestUtils.md5Hex(downstreamVersionFor("vsn2") + downstreamVersionFor("vsn3") + downstreamVersionFor("vsn5a")))),
        VersionDigest(Seq("2010-07-08"), JUL_8_2010_1, DigestUtils.md5Hex(downstreamVersionFor("vsn2") + downstreamVersionFor("vsn3") + downstreamVersionFor("vsn5a")))),
      VersionsFromStore(
        DownstreamVersion(VersionID(abPair, "id2"), categories, JUL_8_2010_1, "vsn2", downstreamVersionFor("vsn2")),
        //DownstreamVersion(VersionID(abPair, "id2"), JUL_8_2010_1, JUL_8_2010_1, "vsn2", downstreamVersionFor("vsn2")),
        DownstreamVersion(VersionID(abPair, "id4"), categories, JUL_8_2010_1, "vsn4", downstreamVersionFor("vsn4")),
        //DownstreamVersion(VersionID(abPair, "id4"), JUL_8_2010_1, JUL_8_2010_1, "vsn4", downstreamVersionFor("vsn4")),
        DownstreamVersion(VersionID(abPair, "id5"), categories, JUL_8_2010_1, "vsn5", downstreamVersionFor("vsn5"))))
        //DownstreamVersion(VersionID(abPair, "id5"), JUL_8_2010_1, JUL_8_2010_1, "vsn5", downstreamVersionFor("vsn5"))))
    expectDownstreamSync(abPair, List(SimpleDateRangeConstraint(JUL_8_2010, endOfDay(JUL_8_2010))),
    //expectDownstreamSync(abPair, DateConstraint(JUL_8_2010, endOfDay(JUL_8_2010)), IndividualGranularity,
      DigestsFromParticipant(
        //VersionDigest("id2", JUL_8_2010_1, JUL_8_2010_1, downstreamVersionFor("vsn2")),
        VersionDigest(Seq("id2"), JUL_8_2010_1, downstreamVersionFor("vsn2")),
        //VersionDigest("id3", JUL_8_2010_1, JUL_8_2010_1, downstreamVersionFor("vsn3")),
        VersionDigest(Seq("id3"), JUL_8_2010_1, downstreamVersionFor("vsn3")),
        //VersionDigest("id5", JUL_8_2010_1, JUL_8_2010_1, downstreamVersionFor("vsn5a"))),
        VersionDigest(Seq("id5"), JUL_8_2010_1, downstreamVersionFor("vsn5a"))),
      VersionsFromStore(
        DownstreamVersion(VersionID(abPair, "id2"), categories, JUL_8_2010_1, "vsn2", downstreamVersionFor("vsn2")),
        //DownstreamVersion(VersionID(abPair, "id2"), JUL_8_2010_1, JUL_8_2010_1, "vsn2", downstreamVersionFor("vsn2")),
        DownstreamVersion(VersionID(abPair, "id4"), categories, JUL_8_2010_1, "vsn4", downstreamVersionFor("vsn4")),
        //DownstreamVersion(VersionID(abPair, "id4"), JUL_8_2010_1, JUL_8_2010_1, "vsn4", downstreamVersionFor("vsn4")),
        DownstreamVersion(VersionID(abPair, "id5"), categories, JUL_8_2010_1, "vsn5", downstreamVersionFor("vsn5"))))
        //DownstreamVersion(VersionID(abPair, "id5"), JUL_8_2010_1, JUL_8_2010_1, "vsn5", downstreamVersionFor("vsn5"))))

    // We should see id3 re-run through the system, and id4 be removed
    expect(usMock.retrieveContent("id3")).andReturn("content3")
    expect(dsMock.generateVersion("content3")).andReturn(ProcessingResponse("id3", categories, "vsn3", downstreamVersionFor("vsn3")))
    //expect(dsMock.generateVersion("content3")).andReturn(ProcessingResponse("id3", JUL_8_2010_1, "vsn3", downstreamVersionFor("vsn3")))
    expect(store.storeDownstreamVersion(VersionID(abPair, "id3"), categories, JUL_8_2010_1, "vsn3", downstreamVersionFor("vsn3"))).
    //expect(store.storeDownstreamVersion(VersionID(abPair, "id3"), JUL_8_2010_1, JUL_8_2010_1, "vsn3", downstreamVersionFor("vsn3"))).
      andReturn(Correlation(null, abPair, "id3", categories, JUL_8_2010_1, timestamp, "vsn3", "vsn3", downstreamVersionFor("vsn3"), false))
      //andReturn(Correlation(null, abPair, "id3", JUL_8_2010_1, JUL_8_2010_1, timestamp, "vsn3", "vsn3", downstreamVersionFor("vsn3"), false))
    expect(store.clearDownstreamVersion(VersionID(abPair, "id4"))).
      andReturn(Correlation.asDeleted(abPair, "id4", new DateTime))
    expect(usMock.retrieveContent("id5")).andReturn("content5")
    expect(dsMock.generateVersion("content5")).andReturn(ProcessingResponse("id5", categories, "vsn5a", downstreamVersionFor("vsn5a")))
    //expect(dsMock.generateVersion("content5")).andReturn(ProcessingResponse("id5", JUL_8_2010_1, "vsn5a", downstreamVersionFor("vsn5a")))
    expect(store.storeDownstreamVersion(VersionID(abPair, "id5"), categories, JUL_8_2010_1, "vsn5a", downstreamVersionFor("vsn5a"))).
    //expect(store.storeDownstreamVersion(VersionID(abPair, "id5"), JUL_8_2010_1, JUL_8_2010_1, "vsn5a", downstreamVersionFor("vsn5a"))).
        andReturn(Correlation(null, abPair, "id3", categories, JUL_8_2010_1, timestamp, "vsn5a", "vsn5a", downstreamVersionFor("vsn5a"), false))
        //andReturn(Correlation(null, abPair, "id3", JUL_8_2010_1, JUL_8_2010_1, timestamp, "vsn5a", "vsn5a", downstreamVersionFor("vsn5a"), false))

    // We should still see an unmatched version check
    expect(store.unmatchedVersions(EasyMock.eq(abPair), EasyMock.eq(Seq(SimpleDateRangeConstraint(START_2009, END_2010))))).
        andReturn(Seq())
    replayAll

    policy.difference(abPair, List(SimpleDateRangeConstraint(START_2009, END_2010)), usMock, dsMock, nullListener)
    verifyAll
  }

  @Test
  def shouldGenerateADifferenceWhenDownstreamResyncFails {
    // Expect only a top-level sync between the pairs
    expectUpstreamSync(abPair, List(SimpleDateRangeConstraint(START_2009, END_2010)),
    //expectUpstreamSync(abPair, DateConstraint(START_2009, END_2010), YearGranularity,
      DigestsFromParticipant(
        //VersionDigest("2009", START_2009, START_2009, DigestUtils.md5Hex("vsn1")),
        VersionDigest(Seq("2009"), START_2009, DigestUtils.md5Hex("vsn1")),
        //VersionDigest("2010", START_2010, START_2010, DigestUtils.md5Hex("vsn2"))),
        VersionDigest(Seq("2010"), START_2010, DigestUtils.md5Hex("vsn2"))),
      VersionsFromStore(
        UpstreamVersion(VersionID(abPair, "id1"), categories, JUN_6_2009_1, "vsn1"),
        //UpstreamVersion(VersionID(abPair, "id1"), JUN_6_2009_1, JUN_6_2009_1, "vsn1"),
        UpstreamVersion(VersionID(abPair, "id2"), categories, JUL_8_2010_1, "vsn2")))
        //UpstreamVersion(VersionID(abPair, "id2"), JUL_8_2010_1, JUL_8_2010_1, "vsn2")))

    expectDownstreamSync(abPair, List(SimpleDateRangeConstraint(START_2009, END_2010)),
    //expectDownstreamSync(abPair, DateConstraint(START_2009, END_2010), YearGranularity,
      DigestsFromParticipant(
        //VersionDigest("2010", START_2010, START_2010, DigestUtils.md5Hex(downstreamVersionFor("vsn2") + downstreamVersionFor("vsn3")))),
        VersionDigest(Seq("2010"), START_2010, DigestUtils.md5Hex(downstreamVersionFor("vsn2") + downstreamVersionFor("vsn3")))),
      VersionsFromStore(
        DownstreamVersion(VersionID(abPair, "id2"), categories, JUL_8_2010_1, "vsn2", downstreamVersionFor("vsn2"))))
        //DownstreamVersion(VersionID(abPair, "id2"), JUL_8_2010_1, JUL_8_2010_1, "vsn2", downstreamVersionFor("vsn2"))))
    expectDownstreamSync(abPair, List(SimpleDateRangeConstraint(START_2010, END_2010)),
    //expectDownstreamSync(abPair, DateConstraint(START_2010, END_2010), MonthGranularity,
      DigestsFromParticipant(
        //VersionDigest("2010-07", JUL_8_2010_1, JUL_8_2010_1, DigestUtils.md5Hex(downstreamVersionFor("vsn2") + downstreamVersionFor("vsn3")))),
        VersionDigest(Seq("2010-07"), JUL_8_2010_1, DigestUtils.md5Hex(downstreamVersionFor("vsn2") + downstreamVersionFor("vsn3")))),
      VersionsFromStore(
        DownstreamVersion(VersionID(abPair, "id2"), categories, JUL_8_2010_1, "vsn2", downstreamVersionFor("vsn2"))))
        //DownstreamVersion(VersionID(abPair, "id2"), JUL_8_2010_1, JUL_8_2010_1, "vsn2", downstreamVersionFor("vsn2"))))
    expectDownstreamSync(abPair, List(SimpleDateRangeConstraint(JUL_2010, END_JUL_2010)),
    //expectDownstreamSync(abPair, DateConstraint(JUL_2010, END_JUL_2010), DayGranularity,
      DigestsFromParticipant(
        //VersionDigest("2010-07-08", JUL_8_2010_1, JUL_8_2010_1, DigestUtils.md5Hex(downstreamVersionFor("vsn2") + downstreamVersionFor("vsn3")))),
        VersionDigest(Seq("2010-07-08"), JUL_8_2010_1, DigestUtils.md5Hex(downstreamVersionFor("vsn2") + downstreamVersionFor("vsn3")))),
      VersionsFromStore(
        DownstreamVersion(VersionID(abPair, "id2"), categories, JUL_8_2010_1, "vsn2", downstreamVersionFor("vsn2"))))
        //DownstreamVersion(VersionID(abPair, "id2"), JUL_8_2010_1, JUL_8_2010_1, "vsn2", downstreamVersionFor("vsn2"))))
    expectDownstreamSync(abPair, List(SimpleDateRangeConstraint(JUL_8_2010, endOfDay(JUL_8_2010))),
    //expectDownstreamSync(abPair, DateConstraint(JUL_8_2010, endOfDay(JUL_8_2010)), IndividualGranularity,
      DigestsFromParticipant(
        VersionDigest(Seq("id2"), JUL_8_2010_1, downstreamVersionFor("vsn2")),
        //VersionDigest("id2", JUL_8_2010_1, JUL_8_2010_1, downstreamVersionFor("vsn2")),
        VersionDigest(Seq("id3"), JUL_8_2010_1, downstreamVersionFor("vsn3"))),
        //VersionDigest("id3", JUL_8_2010_1, JUL_8_2010_1, downstreamVersionFor("vsn3"))),
      VersionsFromStore(
        DownstreamVersion(VersionID(abPair, "id2"), categories, JUL_8_2010_1, "vsn2", downstreamVersionFor("vsn2"))))
        //DownstreamVersion(VersionID(abPair, "id2"), JUL_8_2010_1, JUL_8_2010_1, "vsn2", downstreamVersionFor("vsn2"))))

    // We should see id3 re-run through the system, but not be stored since the version on the downstream is different
    expect(usMock.retrieveContent("id3")).andReturn("content3a")
    expect(dsMock.generateVersion("content3a")).andReturn(ProcessingResponse("id3", categories, "vsn3a", downstreamVersionFor("vsn3a")))
    //expect(dsMock.generateVersion("content3a")).andReturn(ProcessingResponse("id3", JUL_8_2010_1, "vsn3a", downstreamVersionFor("vsn3a")))

    // We should see a difference being generated
    listener.onMismatch(VersionID(abPair, "id3"), JUL_8_2010_1, downstreamVersionFor("vsn3a"), downstreamVersionFor("vsn3")); expectLastCall

    // We should still see an unmatched version check
    expect(store.unmatchedVersions(EasyMock.eq(abPair), EasyMock.eq(Seq(SimpleDateRangeConstraint(START_2009, END_2010))))).
        andReturn(Seq())
    replayAll

    policy.difference(abPair, List(SimpleDateRangeConstraint(START_2009, END_2010)), usMock, dsMock, listener)
    verifyAll
  }
}