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

import scala.collection.mutable.{HashMap}
import scala.collection.JavaConversions._

import org.easymock.EasyMock._
import org.easymock.{IAnswer, EasyMock}
import org.joda.time.DateTime
import org.apache.commons.codec.digest.DigestUtils
import org.junit.Test

import net.lshift.diffa.kernel.util.EasyMockScalaUtils._
import net.lshift.diffa.kernel.participants._
import net.lshift.diffa.kernel.util.Dates._
import net.lshift.diffa.kernel.util.DateUtils._
import net.lshift.diffa.kernel.events._
import net.lshift.diffa.kernel.config.Pair
import net.lshift.diffa.kernel.config.ConfigStore
import net.lshift.diffa.kernel.participants.EasyConstraints._
import net.lshift.diffa.kernel.participants.IntegerCategoryFunction._
import net.lshift.diffa.kernel.util.Conversions._

/**
 * Base class for the various policy tests.
 */
abstract class AbstractPolicyTest {
  // The policy instance under test
  protected def policy:VersionPolicy

  // A method for generating a downstream version based on an upstream version
  protected def downstreamVersionFor(v:String):String

  // The various mocks for listeners and participants
  val usMock = createStrictMock("us", classOf[UpstreamParticipant])
  val dsMock = createStrictMock("ds", classOf[DownstreamParticipant])
  val nullListener = new NullDifferencingListener
  
  val store = createStrictMock("versionStore", classOf[VersionCorrelationStore])
  EasyMock.checkOrder(store, false)   // Store doesn't care about order
  val listener = createStrictMock("listener", classOf[DifferencingListener])

  val configStore = createStrictMock("configStore", classOf[ConfigStore])
  val abPair = "A-B"

  val emptyCategories:Map[String,String] = Map()
  val pair = new Pair(key=abPair, categories=Map("bizDate" -> "date"))

  expect(configStore.getPair(abPair)).andReturn(pair).anyTimes
  replay(configStore)

  protected def replayAll = replay(usMock, dsMock, store, listener)
  protected def verifyAll = verify(usMock, dsMock, store, listener, configStore)

  // Make declaring of sequences of specific types clearer
  def DigestsFromParticipant[T](vals:T*) = Seq[T](vals:_*)
  def VersionsFromStore[T](vals:T*) = Seq[T](vals:_*)

  protected val yearly = YearlyCategoryFunction
  protected val monthly = MonthlyCategoryFunction
  protected val daily = DailyCategoryFunction
  protected val individual = IndividualCategoryFunction

  protected val tens = AutoNarrowingIntegerCategoryFunction(10, 10)
  protected val hundreds = AutoNarrowingIntegerCategoryFunction(100, 10)
  protected val thousands = AutoNarrowingIntegerCategoryFunction(1000, 10)

  def Up(id: String, okey:String, o:Any, s: String): UpstreamVersion = Up(VersionID(abPair, id), okey, o, s)
  def Up(v:VersionID, okey:String, o:Any, s:String): UpstreamVersion = UpstreamVersion(v, Map(okey -> o.toString()), new DateTime, s)
  def Down(id: String, okey:String, o:Any, s1: String, s2: String): DownstreamVersion = Down(VersionID(abPair, id), okey, o, s1, s2)
  def Down(v:VersionID, okey:String, o:Any, s1:String, s2:String): DownstreamVersion = DownstreamVersion(v, Map(okey -> o.toString()), new DateTime, s1, s2)

  def bizDateMap(d:DateTime) = Map("bizDate" -> d.toString())
  def bizDateSeq(d:DateTime) = Seq(d.toString())

  case class PolicyTestData(
    categories: Map[String, String],
    bucketing:Seq[Map[String, CategoryFunction]],
    constraints: Seq[Seq[QueryConstraint]],
    attributes: Seq[Seq[String]],
    downstreamAttributes: Seq[Map[String, String]],
    valueKey: String,
    values: Seq[Any]
  ) {
    def upstreamAttributes = downstreamAttributes
  }

  val dateCategoryData = PolicyTestData(
    categories = Map("bizDate" -> "date"),
    bucketing = Seq(Map("bizDate" -> YearlyCategoryFunction),
                    Map("bizDate" -> MonthlyCategoryFunction),
                    Map("bizDate" -> DailyCategoryFunction),
                    Map("bizDate" -> IndividualCategoryFunction)),
    constraints = Seq(Seq(unconstrainedDate("bizDate")),
                      Seq(dateRangeConstraint("bizDate", START_2010, END_2010)),
                      Seq(dateRangeConstraint("bizDate", JUL_2010, END_JUL_2010)),
                      Seq(dateRangeConstraint("bizDate", JUL_8_2010, endOfDay(JUL_8_2010)))),
    attributes = Seq(Seq("2009"), Seq("2010"), Seq("2010-07"), Seq("2010-07-08")),
    downstreamAttributes = Seq(bizDateMap(JUN_6_2009_1), bizDateMap(JUL_8_2010_1)),
    valueKey = "bizDate",
    values = Seq(JUN_6_2009_1, JUL_8_2010_1)
  )

  val integerCategoryData = PolicyTestData(
    categories = Map("someInt" -> "int"),
    bucketing = Seq(Map("someInt" -> thousands),
                    Map("someInt" -> hundreds),
                    Map("someInt" -> tens),
                    Map("someInt" -> IndividualCategoryFunction)),
    constraints = Seq(Seq(unconstrainedInt("someInt")),
                      Seq(intRangeConstraint("someInt", 2000, 2999)),
                      Seq(intRangeConstraint("someInt", 2300, 2399)),
                      Seq(intRangeConstraint("someInt", 2340, 2349))),
    attributes = Seq(Seq("1000"), Seq("2000"), Seq("2300"), Seq("2340")),
    downstreamAttributes = Seq(Map("someInt" -> "1234"), Map("someInt" -> "2345")),
    valueKey = "someInt",
    values = Seq(1234, 2345)
  )

  @Test
  def shouldOnlySyncTopLevelsWhenParticipantsAndStoresMatchForDateCategories =
    shouldOnlySyncTopLevelsWhenParticipantsAndStoresMatch(dateCategoryData)

  @Test
  def shouldOnlySyncTopLevelsWhenParticipantsAndStoresMatchForIntegerCategories =
    shouldOnlySyncTopLevelsWhenParticipantsAndStoresMatch(integerCategoryData)

  @Test
  def shouldUpdateUpstreamVersionsWhenStoreIsOutOfDateWithUpstreamParticipantForDateCategories =
    shouldUpdateUpstreamVersionsWhenStoreIsOutOfDateWithUpstreamParticipant(dateCategoryData)

  @Test
  def shouldUpdateUpstreamVersionsWhenStoreIsOutOfDateWithUpstreamParticipantForIntegerCategories =
    shouldUpdateUpstreamVersionsWhenStoreIsOutOfDateWithUpstreamParticipant(integerCategoryData)

  @Test
  def shouldReportMismatchesReportedByUnderlyingStoreForDateCategories =
    shouldReportMismatchesReportedByUnderlyingStore(dateCategoryData)

  @Test
  def shouldReportMismatchesReportedByUnderlyingStoreForIntegerCategories =
    shouldReportMismatchesReportedByUnderlyingStore(integerCategoryData)

  @Test
  def shouldStoreUpstreamChangesToCorrelationStoreAndNotifySessionManagerForQuasiLiveDate {
    val lastUpdate = Some(JUL_8_2010_2)
    storeUpstreamChanges(emptyCategories, lastUpdate)
  }

  @Test
  def shouldStoreUpstreamChangesToCorrelationStoreAndNotifySessionManagerWithoutLastUpdate {
    val lastUpdate = None
    storeUpstreamChanges(emptyCategories, lastUpdate)
  }

  @Test
  def shouldStoreDownstreamChangesToCorrelationStoreAndNotifySessionManagerForDateCategories =
    shouldStoreDownstreamChangesToCorrelationStoreAndNotifySessionManager(
      categories = Map("bizDate" -> "date"),
      attributes = bizDateSeq(JUL_8_2010_2),
      downstreamAttributes = bizDateMap(JUL_8_2010_2))

  @Test
  def shouldStoreDownstreamChangesToCorrelationStoreAndNotifySessionManagerForIntegerCategories =
    shouldStoreDownstreamChangesToCorrelationStoreAndNotifySessionManager(
      categories = Map("someInt" -> "int"),
      attributes = Seq("1234"),
      downstreamAttributes = Map("someInt" -> "1234"))

  @Test
  def shouldStoreDownstreamCorrelatedChangesToCorrelationStoreAndNotifySessionManagerForDateCategories =
    shouldStoreDownstreamCorrelatedChangesToCorrelationStoreAndNotifySessionManager(
      categories = Map("bizDate" -> "date"),
      attributes = bizDateSeq(JUL_8_2010_2),
      downstreamAttributes = bizDateMap(JUL_8_2010_2))

  @Test
  def shouldStoreDownstreamCorrelatedChangesToCorrelationStoreAndNotifySessionManagerForIntegerCategories =
    shouldStoreDownstreamCorrelatedChangesToCorrelationStoreAndNotifySessionManager(
      categories = Map("someInt" -> "int"),
      attributes = Seq("1234"),
      downstreamAttributes = Map("someInt" -> "1234"))

  @Test
  def shouldRaiseMatchEventWhenDownstreamCausesMatchOfUpstreamForDateCategories =
    shouldRaiseMatchEventWhenDownstreamCausesMatchOfUpstream(
      categories = Map("bizDate" -> "date"),
      attributes = bizDateSeq(JUL_8_2010_2),
      downstreamAttributes = bizDateMap(JUL_8_2010_2))

  @Test
  def shouldRaiseMatchEventWhenDownstreamCausesMatchOfUpstreamForIntegerCategories =
    shouldRaiseMatchEventWhenDownstreamCausesMatchOfUpstream(
      categories = Map("someInt" -> "int"),
      attributes = Seq("1234"),
      downstreamAttributes = Map("someInt" -> "1234"))

  protected def shouldOnlySyncTopLevelsWhenParticipantsAndStoresMatch(testData: PolicyTestData) {
    pair.categories = testData.categories
    // Expect only a top-level sync between the pairs
    expectUpstreamAggregateSync(testData.bucketing(0), testData.constraints(0),
      DigestsFromParticipant(AggregateDigest(testData.attributes(0), null, DigestUtils.md5Hex("vsn1")),
                             AggregateDigest(testData.attributes(1), null, DigestUtils.md5Hex("vsn2"))),
      VersionsFromStore(Up("id1", testData.valueKey, testData.values(0), "vsn1"), Up("id2", testData.valueKey, testData.values(1), "vsn2")))
    expectDownstreamAggregateSync(testData.bucketing(0), testData.constraints(0),
      DigestsFromParticipant(
        AggregateDigest(testData.attributes(0),  null, DigestUtils.md5Hex(downstreamVersionFor("vsn1"))),
        AggregateDigest(testData.attributes(1), null, DigestUtils.md5Hex(downstreamVersionFor("vsn2")))),
      VersionsFromStore(Down("id1", testData.valueKey, testData.values(0), "vsn1", downstreamVersionFor("vsn1")),
                        Down("id2", testData.valueKey, testData.values(1), "vsn2", downstreamVersionFor("vsn2"))))

    // We should still see an unmatched version check
    expect(store.unmatchedVersions(EasyMock.eq(abPair), EasyMock.eq(testData.constraints(0)))).andReturn(Seq())
    replayAll

    policy.difference(abPair, usMock, dsMock, nullListener)
    verifyAll
  }
  
  protected def shouldUpdateUpstreamVersionsWhenStoreIsOutOfDateWithUpstreamParticipant(testData: PolicyTestData) {
    pair.categories = testData.categories
    val timestamp = new DateTime
    // Expect only a top-level sync between the pairs
    expectUpstreamAggregateSync(testData.bucketing(0), testData.constraints(0),
      DigestsFromParticipant(
        AggregateDigest(testData.attributes(0), START_2009, DigestUtils.md5Hex("vsn1")),
        AggregateDigest(testData.attributes(1), START_2010, DigestUtils.md5Hex("vsn2new" + "vsn4"))),
      VersionsFromStore(
        Up("id1", testData.valueKey, testData.values(0), "vsn1"),
        Up("id2", testData.valueKey, testData.values(1), "vsn2"),
        Up("id3", testData.valueKey, testData.values(1), "vsn3")))
    expectUpstreamAggregateSync(testData.bucketing(1), testData.constraints(1),
      DigestsFromParticipant(
        AggregateDigest(testData.attributes(2), JUL_8_2010_1, DigestUtils.md5Hex("vsn2new" + "vsn4"))),
      VersionsFromStore(
        Up("id2", testData.valueKey, testData.values(1), "vsn2"),
        Up("id3", testData.valueKey, testData.values(1), "vsn3")))
    expectUpstreamAggregateSync(testData.bucketing(2), testData.constraints(2),
      DigestsFromParticipant(
        AggregateDigest(testData.attributes(3), JUL_8_2010_1, DigestUtils.md5Hex("vsn2new"  + "vsn4"))),
      VersionsFromStore(
        Up("id2", testData.valueKey, testData.values(1), "vsn2"),
        Up("id3",testData.valueKey,  testData.values(1), "vsn3")))
    expectUpstreamEntitySync(testData.constraints(3),
      DigestsFromParticipant(
        EntityVersion("id2", Seq(testData.values(1).toString), JUL_8_2010_1, "vsn2new"),
        EntityVersion("id4", Seq(testData.values(1).toString), JUL_8_2010_1, "vsn4")),
      VersionsFromStore(
        Up("id2", testData.valueKey, testData.values(1), "vsn2"),
        Up("id3", testData.valueKey, testData.values(1), "vsn3")))

    expectDownstreamAggregateSync(testData.bucketing(0), testData.constraints(0),
      DigestsFromParticipant(
        AggregateDigest(testData.attributes(0), START_2009, DigestUtils.md5Hex(downstreamVersionFor("vsn1"))),
        AggregateDigest(testData.attributes(1), START_2010, DigestUtils.md5Hex(downstreamVersionFor("vsn2")))),
      VersionsFromStore(
        Down("id1", testData.valueKey, testData.values(0), "vsn1", downstreamVersionFor("vsn1")),
        Down("id2", testData.valueKey, testData.values(1), "vsn2", downstreamVersionFor("vsn2"))))

    // The policy should update the version for id2, remove id3 and add id4
    expect(store.storeUpstreamVersion(VersionID(abPair, "id2"), testData.upstreamAttributes(1), JUL_8_2010_1, "vsn2new")).
      andReturn(Correlation(null, abPair, "id3", testData.upstreamAttributes(1), null, JUL_8_2010_1, timestamp, "vsn2new", "vsn2", downstreamVersionFor("vsn2"), false))
    expect(store.clearUpstreamVersion(VersionID(abPair, "id3"))).
      andReturn(Correlation.asDeleted(abPair, "id3", new DateTime))
    expect(store.storeUpstreamVersion(VersionID(abPair, "id4"), testData.upstreamAttributes(1), JUL_8_2010_1, "vsn4")).
      andReturn(Correlation(null, abPair, "id4", testData.upstreamAttributes(1), null, JUL_8_2010_1, timestamp, downstreamVersionFor("vsn2"), null, null, false))

    // Don't report any unmatched versions
    expect(store.unmatchedVersions(EasyMock.eq(abPair), EasyMock.eq(testData.constraints(0)))).andReturn(Seq())
    replayAll

    policy.difference(abPair, usMock, dsMock, nullListener)
    verifyAll
  }

  protected def shouldReportMismatchesReportedByUnderlyingStore(testData: PolicyTestData) {
    pair.categories = testData.categories
    val timestamp = new DateTime
    // Expect only a top-level sync between the pairs
    expectUpstreamAggregateSync(testData.bucketing(0), testData.constraints(0),
      DigestsFromParticipant(
        AggregateDigest(testData.attributes(0), START_2009, DigestUtils.md5Hex("vsn1")),
        AggregateDigest(testData.attributes(1), START_2010, DigestUtils.md5Hex("vsn2"))),
      VersionsFromStore(
        Up("id1", testData.valueKey, testData.values(0), "vsn1"),
        Up("id2", testData.valueKey, testData.values(1), "vsn2")))
    expectDownstreamAggregateSync(testData.bucketing(0), testData.constraints(0),
      DigestsFromParticipant(
        AggregateDigest(testData.attributes(0), START_2009, DigestUtils.md5Hex(downstreamVersionFor("vsn1a"))),
        AggregateDigest(testData.attributes(1), START_2010, DigestUtils.md5Hex(downstreamVersionFor("vsn2a")))),
      VersionsFromStore(Down("id1", testData.valueKey, testData.values(0), "vsn1a", downstreamVersionFor("vsn1a")),
                        Down("id2", testData.valueKey, testData.values(1), "vsn2a", downstreamVersionFor("vsn2a"))))

    // If the version check returns mismatches, we should see differences generated
    expect(store.unmatchedVersions(EasyMock.eq(abPair), EasyMock.eq(testData.constraints(0)))).andReturn(Seq(
      Correlation(null, abPair, "id1", testData.upstreamAttributes(0), emptyCategories, JUN_6_2009_1, timestamp, "vsn1", "vsn1a", "vsn3", false),
      Correlation(null, abPair, "id2", testData.upstreamAttributes(1), emptyCategories, JUL_8_2010_1, timestamp, "vsn2", "vsn2a", "vsn4", false)))
    listener.onMismatch(VersionID(abPair, "id1"), JUN_6_2009_1, "vsn1", "vsn1a"); expectLastCall
    listener.onMismatch(VersionID(abPair, "id2"), JUL_8_2010_1, "vsn2", "vsn2a"); expectLastCall

    replayAll

    policy.difference(abPair, usMock, dsMock, listener)
    verifyAll
  }

  /**
   * This is a utility function that allows a kind of virtual date mode for testing
   * historical submissions
   */
  def storeUpstreamChanges(cats:Map[String,String], lastUpdate:Option[DateTime]) {
    val timestamp = new DateTime
    val (update, observationDate, f) = lastUpdate match {
      case None     => (timestamp, null, () =>
        store.storeUpstreamVersion(EasyMock.eq(VersionID(abPair, "id1")), EasyMock.eq(cats),
                                   between(timestamp, timestamp.plusMillis(200)), EasyMock.eq("vsn1")))
      case Some(x)  => (x, x, () => store.storeUpstreamVersion(VersionID(abPair, "id1"), cats, x, "vsn1"))
    }
    expect(f()).andReturn(Correlation(null, abPair, "id1", cats, null, update, timestamp, "vsn1", null, null, false))
    listener.onMismatch(VersionID(abPair, "id1"), update, "vsn1", null); expectLastCall
    replayAll

    policy.onChange(UpstreamPairChangeEvent(VersionID(abPair, "id1"), cats.values.toSeq, observationDate, "vsn1"))
    verifyAll
  }

  protected def shouldStoreDownstreamChangesToCorrelationStoreAndNotifySessionManager(
    categories: Map[String, String],
    attributes: Seq[String],
    downstreamAttributes: Map[String, String]
  ) {
    pair.categories = categories
    val timestamp = new DateTime
    expect(store.storeDownstreamVersion(VersionID(abPair, "id1"), downstreamAttributes, JUL_8_2010_2, "vsn1", "vsn1")).
      andReturn(Correlation(null, abPair, "id1", downstreamAttributes, categories, JUL_8_2010_2, timestamp, null, "vsn1", "vsn1", false))
    listener.onMismatch(VersionID(abPair, "id1"), JUL_8_2010_2, null, "vsn1"); expectLastCall
    replayAll

    policy.onChange(DownstreamPairChangeEvent(VersionID(abPair, "id1"), attributes, JUL_8_2010_2, "vsn1"))
    verifyAll
  }

  protected def shouldStoreDownstreamCorrelatedChangesToCorrelationStoreAndNotifySessionManager(
    categories: Map[String, String],
    attributes: Seq[String],
    downstreamAttributes: Map[String, String]
  ) {
    pair.categories = categories
    val timestamp = new DateTime
    expect(store.storeDownstreamVersion(VersionID(abPair, "id1"), downstreamAttributes, JUL_8_2010_2, "vsn1", "vsn2")).
      andReturn(Correlation(null, abPair, "id1", null, downstreamAttributes, JUL_8_2010_2, timestamp, null, "vsn1", "vsn1", false))
    listener.onMismatch(VersionID(abPair, "id1"), JUL_8_2010_2, null, "vsn1"); expectLastCall
    replayAll

    policy.onChange(DownstreamCorrelatedPairChangeEvent(VersionID(abPair, "id1"), attributes, JUL_8_2010_2, "vsn1", "vsn2"))
    verifyAll
  }

  protected def shouldRaiseMatchEventWhenDownstreamCausesMatchOfUpstream(
    categories: Map[String, String],
    attributes: Seq[String],
    downstreamAttributes: Map[String, String]
  ) {
    pair.categories = categories
    val timestamp = new DateTime
    expect(store.storeDownstreamVersion(VersionID(abPair, "id1"), downstreamAttributes, JUL_8_2010_2, "vsn1", "vsn2")).
      andReturn(Correlation(null, abPair, "id1", null, downstreamAttributes, JUL_8_2010_2, timestamp, "vsn1", "vsn1", "vsn2", true))
    listener.onMatch(VersionID(abPair, "id1"), "vsn1"); expectLastCall
    replayAll

    policy.onChange(DownstreamCorrelatedPairChangeEvent(VersionID(abPair, "id1"), attributes, JUL_8_2010_2, "vsn1", "vsn2"))
    verifyAll
  }

  //
  // Standard Types
  //

  protected case class UpstreamVersion(id:VersionID, attributes:Map[String, String], lastUpdate:DateTime, vsn:String)
  protected case class UpstreamVersionAnswer(hs:Seq[UpstreamVersion]) extends IAnswer[Unit] {
    def answer {
      val args = EasyMock.getCurrentArguments
      val cb = args(2).asInstanceOf[Function4[VersionID, Map[String, String], DateTime, String, Unit]]

      hs.foreach { case UpstreamVersion(id, attributes, lastUpdate, vsn) =>
        cb(id, attributes, lastUpdate, vsn)
      }
    }
  }
  protected case class DownstreamVersion(id:VersionID, attributes:Map[String, String], lastUpdate:DateTime, usvn:String, dsvn:String)
  protected case class DownstreamVersionAnswer(hs:Seq[DownstreamVersion]) extends IAnswer[Unit] {
    def answer {
      val args = EasyMock.getCurrentArguments
      val cb = args(2).asInstanceOf[Function5[VersionID, Map[String, String], DateTime, String, String, Unit]]

      hs.foreach { case DownstreamVersion(id, attributes, lastUpdate, uvsn, dvsn) =>
        cb(id, attributes, lastUpdate, uvsn, dvsn)
      }
    }
  }

  protected def expectUpstreamAggregateSync(bucketing:Map[String, CategoryFunction], constraints: Seq[QueryConstraint], partResp: Seq[AggregateDigest],
                                            storeResp: Seq[UpstreamVersion]) {
    expectUpstreamAggregateSync(abPair, bucketing, constraints, partResp, storeResp: Seq[UpstreamVersion])
  }

  protected def expectUpstreamAggregateSync(pair:String, bucketing:Map[String, CategoryFunction], constraints:Seq[QueryConstraint], partResp:Seq[AggregateDigest], storeResp:Seq[UpstreamVersion]) {
    expect(usMock.queryAggregateDigests(bucketing, constraints)).andReturn(partResp)
    store.queryUpstreams(EasyMock.eq(pair), EasyMock.eq(constraints), anyUnitF4)
      expectLastCall[Unit].andAnswer(UpstreamVersionAnswer(storeResp))
  }

  protected def expectDownstreamAggregateSync(bucketing:Map[String, CategoryFunction], constraints: Seq[QueryConstraint], partResp: Seq[AggregateDigest],
                                              storeResp: Seq[DownstreamVersion]) {
    expectDownstreamAggregateSync(abPair, bucketing, constraints, partResp, storeResp)
  }

  protected def expectDownstreamAggregateSync(pair:String, bucketing:Map[String, CategoryFunction], constraints:Seq[QueryConstraint], partResp:Seq[AggregateDigest], storeResp:Seq[DownstreamVersion]) {
    expect(dsMock.queryAggregateDigests(bucketing, constraints)).andReturn(partResp)
    store.queryDownstreams(EasyMock.eq(pair), EasyMock.eq(constraints), anyUnitF5)
      expectLastCall[Unit].andAnswer(DownstreamVersionAnswer(storeResp))
  }

  protected def expectUpstreamEntitySync(constraints: Seq[QueryConstraint], partResp: Seq[EntityVersion],
                                         storeResp: Seq[UpstreamVersion]) {
    expectUpstreamEntitySync(abPair, constraints, partResp, storeResp)
  }

  protected def expectUpstreamEntitySync(pair:String, constraints:Seq[QueryConstraint], partResp:Seq[EntityVersion], storeResp:Seq[UpstreamVersion]) {
    val pairDef = configStore.getPair(pair)
    expect(usMock.queryEntityVersions(constraints)).andReturn(partResp)
    val correlations = storeResp.map(r => {
      val c = new Correlation()
      c.id = r.id.id
      c.upstreamAttributes = r.attributes
      c.lastUpdate = r.lastUpdate
      c.upstreamVsn = r.vsn
      c
    })

    expect(store.queryUpstreams(EasyMock.eq(pair), EasyMock.eq(constraints))).andReturn(correlations)
  }
  protected def expectDownstreamEntitySync2(pair:String, constraints:Seq[QueryConstraint], partResp:Seq[EntityVersion], storeResp:Seq[DownstreamVersion]) {
    val pairDef = configStore.getPair(pair)
    expect(dsMock.queryEntityVersions(constraints)).andReturn(partResp)
    val correlations = storeResp.map(r => {
      val c = new Correlation      
      c.id = r.id.id
      c.downstreamAttributes = r.attributes
      c.lastUpdate = r.lastUpdate
      c.downstreamDVsn = r.dsvn
      c
    })

    expect(store.queryDownstreams(EasyMock.eq(pair), EasyMock.eq(constraints))).andReturn(correlations)
  }
}