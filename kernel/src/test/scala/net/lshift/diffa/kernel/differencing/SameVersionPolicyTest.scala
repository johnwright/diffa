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

import java.lang.String
import scala.collection.mutable.Map

import org.junit.Test
import org.easymock.EasyMock._
import org.joda.time.DateTime
import org.easymock.EasyMock
import org.apache.commons.codec.digest.DigestUtils

import net.lshift.diffa.kernel.util.DateUtils._
import net.lshift.diffa.kernel.participants._
import net.lshift.diffa.kernel.events.VersionID
import net.lshift.diffa.kernel.util.Dates._
import net.lshift.diffa.kernel.participants.EasyConstraints._
import net.lshift.diffa.kernel.util.Conversions._

/**
 * Test cases for the same version policy.
 */
class SameVersionPolicyTest extends AbstractPolicyTest {
  val policy = new SameVersionPolicy(store, listener, configStore)

  def downstreamVersionFor(v:String) = v

  protected def shouldUpdateDownstreamVersionsWhenStoreIsOutOfDateWithDownstreamParticipant(
    categories: Map[String, String],
    constraints: (Seq[QueryConstraint], Seq[QueryConstraint], Seq[QueryConstraint], Seq[QueryConstraint]),
    attributes: (Seq[String], Seq[String], Seq[String], Seq[String]),
    downstreamAttributes: Map[String, String],
    values: (Any, Any)
  ) {
    pair.categories = categories
    val timestamp = new DateTime
    val (constraints1, constraints2, constraints3, constraints4) = constraints
    val (attributes1, attributes2, attributes3, attributes4) = attributes
    val (value1, value2) = values
    // Expect only a top-level sync for the upstream, but a full sync for the downstream
    expectUpstreamAggregateSync(constraints1,
      DigestsFromParticipant(
        AggregateDigest(attributes1, START_2009, DigestUtils.md5Hex("vsn1")),
        AggregateDigest(attributes2, START_2010, DigestUtils.md5Hex("vsn2"))),
      VersionsFromStore(
        Up("id1", value1, "vsn1"),
        Up("id2", value2, "vsn2")))

    expectDownstreamAggregateSync(constraints1,
      DigestsFromParticipant(
        AggregateDigest(attributes1, START_2009, DigestUtils.md5Hex(downstreamVersionFor("vsn1"))),
        AggregateDigest(attributes2, START_2010, DigestUtils.md5Hex(downstreamVersionFor("vsn2") + downstreamVersionFor("vsn3")))),
      VersionsFromStore(
        Down("id1", value1, "vsn1", downstreamVersionFor("vsn1")),
        Down("id2", value2, "vsn2", downstreamVersionFor("vsn2")),
        Down("id4", value2, "vsn4", downstreamVersionFor("vsn4"))))
    expectDownstreamAggregateSync(constraints2,
      DigestsFromParticipant(
        AggregateDigest(attributes3, JUL_8_2010_1, DigestUtils.md5Hex(downstreamVersionFor("vsn2") + downstreamVersionFor("vsn3")))),
      VersionsFromStore(
        Down("id2", value2, "vsn2", downstreamVersionFor("vsn2")),
        Down("id4", value2, "vsn4", downstreamVersionFor("vsn4"))))
    expectDownstreamAggregateSync(constraints3,
      DigestsFromParticipant(
        AggregateDigest(attributes4, JUL_8_2010_1, DigestUtils.md5Hex(downstreamVersionFor("vsn2") + downstreamVersionFor("vsn3")))),
      VersionsFromStore(
        Down("id2", value2, "vsn2", downstreamVersionFor("vsn2")),
        Down("id4", value2, "vsn4", downstreamVersionFor("vsn4"))))
    expectDownstreamEntitySync2(abPair, constraints4,
      DigestsFromParticipant(
        EntityVersion("id2", Seq(value2.toString), JUL_8_2010_1, downstreamVersionFor("vsn2")),
        EntityVersion("id3", Seq(value2.toString), JUL_8_2010_1, downstreamVersionFor("vsn3"))),
      VersionsFromStore(
        Down("id2", value2, "vsn2", downstreamVersionFor("vsn2")),
        Down("id4", value2, "vsn4", downstreamVersionFor("vsn4"))))

    // We should see id3 be updated, and id4 be removed
    expect(store.storeDownstreamVersion(VersionID(abPair, "id3"), downstreamAttributes, JUL_8_2010_1, "vsn3", downstreamVersionFor("vsn3"))).
      andReturn(Correlation(null, abPair, "id3", null, downstreamAttributes,JUL_8_2010_1, timestamp, "vsn3", "vsn3", downstreamVersionFor("vsn3"), false))
    expect(store.clearDownstreamVersion(VersionID(abPair, "id4"))).
      andReturn(Correlation.asDeleted(abPair, "id4", new DateTime))

    // We should still see an unmatched version check
    expect(store.unmatchedVersions(EasyMock.eq(abPair), EasyMock.eq(constraints1))).andReturn(Seq())
    replayAll

    policy.difference(abPair, usMock, dsMock, nullListener)
    verifyAll
  }

  @Test
  def shouldUpdateDownstreamVersionsWhenStoreIsOutOfDateWithDownstreamParticipantForDateCategories =
    shouldUpdateDownstreamVersionsWhenStoreIsOutOfDateWithDownstreamParticipant(
      categories = Map("bizDate" -> "date"),
      constraints = (Seq(unconstrainedDate(YearlyCategoryFunction)),
                     Seq(dateRangeConstraint(START_2010, END_2010, monthly)),
                     Seq(dateRangeConstraint(JUL_2010, END_JUL_2010, daily)),
                     Seq(dateRangeConstraint(JUL_8_2010, endOfDay(JUL_8_2010), individual))),
      attributes = (Seq("2009"), Seq("2010"), Seq("2010-07"), Seq("2010-07-08")),
      downstreamAttributes = bizDateMap(JUL_8_2010_1),
      values = (JUN_6_2009_1, JUL_8_2010_1))

  @Test
  def shouldUpdateDownstreamVersionsWhenStoreIsOutOfDateWithDownstreamParticipantForIntegerCategories =
    shouldUpdateDownstreamVersionsWhenStoreIsOutOfDateWithDownstreamParticipant(
      categories = Map("someInt" -> "int"),
      constraints = (Seq(unconstrainedInt(ThousandsCategoryFunction)),
                     Seq(intRangeConstraint(2000, 2999, HundredsCategoryFunction)),
                     Seq(intRangeConstraint(2300, 2399, TensCategoryFunction)),
                     Seq(intRangeConstraint(2340, 2349, IndividualCategoryFunction))),
      attributes = (Seq("1000"), Seq("2000"), Seq("2300"), Seq("2340")),
      downstreamAttributes = Map("someInt" -> "2345"),
      values = (1234, 2345))
}