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
import scala.concurrent.SyncVar

import org.easymock.EasyMock._
import org.easymock.{IAnswer, EasyMock}
import org.joda.time.DateTime
import org.apache.commons.codec.digest.DigestUtils
import org.junit.Test

import net.lshift.diffa.kernel.util.EasyMockScalaUtils._
import net.lshift.diffa.kernel.participants._
import net.lshift.diffa.kernel.util.FullDateTimes._
import net.lshift.diffa.kernel.util.DateUtils._
import net.lshift.diffa.kernel.events._
import net.lshift.diffa.kernel.participants.EasyConstraints._
import net.lshift.diffa.kernel.participants.IntegerCategoryFunction._
import net.lshift.diffa.kernel.config._
import net.lshift.diffa.kernel.util.NonCancellingFeedbackHandle

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

  val writer = createMock("writer", classOf[LimitedVersionCorrelationWriter])
  val store = createMock("versionStore", classOf[VersionCorrelationStore])

  val stores = new VersionCorrelationStoreFactory {
    def apply(pairKey: String) = store
    def remove(pairKey: String) {}
    def close {}
  }

  val feedbackHandle = new NonCancellingFeedbackHandle

  val listener = createStrictMock("listener", classOf[DifferencingListener])

  val configStore = createStrictMock("configStore", classOf[ConfigStore])
  val abPair = "A-B"

  val emptyAttributes:Map[String,TypedAttribute] = Map()
  val emptyStrAttributes:Map[String,String] = Map()

  val dateCategoryDescriptor = new RangeCategoryDescriptor("datetime")
  val intCategoryDescriptor = new RangeCategoryDescriptor("int")

  val pair = new Pair(key=abPair, upstream=new Endpoint(categories=Map("bizDate" -> dateCategoryDescriptor)), downstream=new Endpoint(categories=Map("bizDate" -> dateCategoryDescriptor)))

  expect(configStore.getPair(abPair)).andReturn(pair).anyTimes
  replay(configStore)

  protected def replayAll = replay(usMock, dsMock, store, writer, listener)
  protected def verifyAll = verify(usMock, dsMock, store, writer, listener, configStore)

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

  def bizDateMap(d:DateTime) = Map("bizDate" -> DateTimeAttribute(d))
  def bizDateSeq(d:DateTime) = Seq(d.toString())

  case class PolicyTestData(
    upstreamCategories: Map[String, CategoryDescriptor],
    downstreamCategories: Map[String, CategoryDescriptor],
    bucketing:Seq[Map[String, CategoryFunction]],
    constraints: Seq[Seq[QueryConstraint]],
    attributes: Seq[Seq[String]],
    downstreamAttributes: Seq[Map[String, TypedAttribute]],
    valueKey: String,
    values: Seq[Any]
  ) {
    def upstreamAttributes = downstreamAttributes
  }

  val dateCategoryData = PolicyTestData(
    upstreamCategories = Map("bizDate" -> dateCategoryDescriptor),
    downstreamCategories = Map("bizDate" -> dateCategoryDescriptor),
    bucketing = Seq(Map("bizDate" -> YearlyCategoryFunction),
                    Map("bizDate" -> MonthlyCategoryFunction),
                    Map("bizDate" -> DailyCategoryFunction),
                    Map("bizDate" -> IndividualCategoryFunction)),
    constraints = Seq(Seq(unconstrainedDateTime("bizDate")),
                      Seq(DateTimeRangeConstraint("bizDate", START_2010, END_2010)),
                      Seq(DateTimeRangeConstraint("bizDate", JUL_2010, END_JUL_2010)),
                      Seq(DateTimeRangeConstraint("bizDate", JUL_8_2010, endOfDay(JUL_8_2010)))),
    attributes = Seq(Seq("2009"), Seq("2010"), Seq("2010-07"), Seq("2010-07-08")),
    downstreamAttributes = Seq(bizDateMap(JUN_6_2009_1), bizDateMap(JUL_8_2010_1)),
    valueKey = "bizDate",
    values = Seq(JUN_6_2009_1, JUL_8_2010_1)
  )

  val integerCategoryData = PolicyTestData(
    upstreamCategories = Map("someInt" -> intCategoryDescriptor),
    downstreamCategories = Map("someInt" -> intCategoryDescriptor),
    bucketing = Seq(Map("someInt" -> thousands),
                    Map("someInt" -> hundreds),
                    Map("someInt" -> tens),
                    Map("someInt" -> IndividualCategoryFunction)),
    constraints = Seq(Seq(unconstrainedInt("someInt")),
                      Seq(IntegerRangeConstraint("someInt", 2000, 2999)),
                      Seq(IntegerRangeConstraint("someInt", 2300, 2399)),
                      Seq(IntegerRangeConstraint("someInt", 2340, 2349))),
    attributes = Seq(Seq("1000"), Seq("2000"), Seq("2300"), Seq("2340")),
    downstreamAttributes = Seq(Map("someInt" -> IntegerAttribute(1234)), Map("someInt" -> IntegerAttribute(2345))),
    valueKey = "someInt",
    values = Seq(1234, 2345)
  )

  @Test
  def shouldReportMismatchesReportedByUnderlyingStoreForDateCategories =
    shouldReportMismatchesReportedByUnderlyingStore(dateCategoryData)

  @Test
  def shouldReportMismatchesReportedByUnderlyingStoreForIntegerCategories =
    shouldReportMismatchesReportedByUnderlyingStore(integerCategoryData)

  @Test
  def shouldStoreUpstreamChangesToCorrelationStoreAndNotifySessionManagerForQuasiLiveDate {
    val lastUpdate = Some(JUL_8_2010_2)
    storeUpstreamChanges(emptyAttributes, lastUpdate)
  }

  @Test
  def shouldStoreUpstreamChangesToCorrelationStoreAndNotifySessionManagerWithoutLastUpdate {
    val lastUpdate = None
    storeUpstreamChanges(emptyAttributes, lastUpdate)
  }

  @Test
  def shouldStoreDownstreamChangesToCorrelationStoreAndNotifySessionManagerForDateCategories =
    shouldStoreDownstreamChangesToCorrelationStoreAndNotifySessionManager(
      upstreamCategories = Map("bizDate" -> dateCategoryDescriptor),
      downstreamCategories = Map("bizDate" -> dateCategoryDescriptor),
      attributes = bizDateSeq(JUL_8_2010_2),
      downstreamAttributes = bizDateMap(JUL_8_2010_2))

  @Test
  def shouldStoreDownstreamChangesToCorrelationStoreAndNotifySessionManagerForIntegerCategories =
    shouldStoreDownstreamChangesToCorrelationStoreAndNotifySessionManager(
      upstreamCategories = Map("someInt" -> intCategoryDescriptor),
      downstreamCategories = Map("someInt" -> intCategoryDescriptor),
      attributes = Seq("1234"),
      downstreamAttributes = Map("someInt" -> IntegerAttribute(1234)))

  @Test
  def shouldStoreDownstreamCorrelatedChangesToCorrelationStoreAndNotifySessionManagerForDateCategories =
    shouldStoreDownstreamCorrelatedChangesToCorrelationStoreAndNotifySessionManager(
      upstreamCategories = Map("bizDate" -> dateCategoryDescriptor),
      downstreamCategories = Map("bizDate" -> dateCategoryDescriptor),
      attributes = bizDateSeq(JUL_8_2010_2),
      downstreamAttributes = bizDateMap(JUL_8_2010_2))

  @Test
  def shouldStoreDownstreamCorrelatedChangesToCorrelationStoreAndNotifySessionManagerForIntegerCategories =
    shouldStoreDownstreamCorrelatedChangesToCorrelationStoreAndNotifySessionManager(
      upstreamCategories = Map("someInt" -> intCategoryDescriptor),
      downstreamCategories = Map("someInt" -> intCategoryDescriptor),
      attributes = Seq("1234"),
      downstreamAttributes = Map("someInt" -> IntegerAttribute(1234)))

  @Test
  def shouldRaiseMatchEventWhenDownstreamCausesMatchOfUpstreamForDateCategories =
    shouldRaiseMatchEventWhenDownstreamCausesMatchOfUpstream(
      upstreamCategories = Map("bizDate" -> dateCategoryDescriptor),
      downstreamCategories = Map("bizDate" -> dateCategoryDescriptor),
      attributes = bizDateSeq(JUL_8_2010_2),
      downstreamAttributes = bizDateMap(JUL_8_2010_2))

  @Test
  def shouldRaiseMatchEventWhenDownstreamCausesMatchOfUpstreamForIntegerCategories =
    shouldRaiseMatchEventWhenDownstreamCausesMatchOfUpstream(
      upstreamCategories = Map("someInt" -> intCategoryDescriptor),
      downstreamCategories = Map("someInt" -> intCategoryDescriptor),
      attributes = Seq("1234"),
      downstreamAttributes = Map("someInt" -> IntegerAttribute(1234)))

  protected def shouldReportMismatchesReportedByUnderlyingStore(testData: PolicyTestData) {
    pair.upstream.categories = testData.upstreamCategories
    pair.downstream.categories = testData.downstreamCategories
    val timestamp = new DateTime
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
        AggregateDigest(testData.attributes(0), DigestUtils.md5Hex(downstreamVersionFor("vsn1a"))),
        AggregateDigest(testData.attributes(1), DigestUtils.md5Hex(downstreamVersionFor("vsn2a")))),
      VersionsFromStore(Down("id1", testData.valueKey, testData.values(0), "vsn1a", downstreamVersionFor("vsn1a")),
                        Down("id2", testData.valueKey, testData.values(1), "vsn2a", downstreamVersionFor("vsn2a"))))

    // If the version check returns mismatches, we should see differences generated
    expect(store.unmatchedVersions(EasyMock.eq(testData.constraints(0)), EasyMock.eq(testData.constraints(0)))).andReturn(Seq(
      Correlation(null, abPair, "id1", toStrMap(testData.upstreamAttributes(0)), emptyStrAttributes, JUN_6_2009_1, timestamp, "vsn1", "vsn1a", "vsn3", false),
      Correlation(null, abPair, "id2", toStrMap(testData.upstreamAttributes(1)), emptyStrAttributes, JUL_8_2010_1, timestamp, "vsn2", "vsn2a", "vsn4", false)))
    listener.onMismatch(VersionID(abPair, "id1"), JUN_6_2009_1, "vsn1", "vsn1a", LiveWindow); expectLastCall
    listener.onMismatch(VersionID(abPair, "id2"), JUL_8_2010_1, "vsn2", "vsn2a", LiveWindow); expectLastCall

    replayAll

    policy.scanUpstream(abPair, writer, usMock, nullListener, feedbackHandle)
    policy.scanDownstream(abPair, writer, usMock, dsMock, listener, feedbackHandle)
    policy.difference(abPair, listener)

    verifyAll
  }

  /**
   * This is a utility function that allows a kind of virtual date mode for testing
   * historical submissions
   */
  def storeUpstreamChanges(attrs:Map[String,TypedAttribute], lastUpdate:Option[DateTime]) {
    val timestamp = new DateTime
    val (update, observationDate, f) = lastUpdate match {
      case None     => (timestamp, null, () =>
        writer.storeUpstreamVersion(EasyMock.eq(VersionID(abPair, "id1")), EasyMock.eq(attrs),
                                     between(timestamp, timestamp.plusMillis(200)), EasyMock.eq("vsn1")))
      case Some(x)  => (x, x, () => writer.storeUpstreamVersion(VersionID(abPair, "id1"), attrs, x, "vsn1"))
    }
    expect(f()).andReturn(Correlation(null, abPair, "id1", toStrMap(attrs), null, update, timestamp, "vsn1", null, null, false))
    listener.onMismatch(VersionID(abPair, "id1"), update, "vsn1", null, LiveWindow); expectLastCall
    replayAll

    policy.onChange(writer, UpstreamPairChangeEvent(VersionID(abPair, "id1"), toStrMap(attrs).values.toSeq, observationDate, "vsn1"))
    verifyAll
  }

  protected def shouldStoreDownstreamChangesToCorrelationStoreAndNotifySessionManager(
    upstreamCategories: Map[String, CategoryDescriptor],
    downstreamCategories: Map[String, CategoryDescriptor],
    attributes: Seq[String],
    downstreamAttributes: Map[String, TypedAttribute]
  ) {
    pair.upstream.categories = upstreamCategories
    pair.downstream.categories = downstreamCategories
    val timestamp = new DateTime

    expect(writer.storeDownstreamVersion(VersionID(abPair, "id1"), downstreamAttributes, JUL_8_2010_2, "vsn1", "vsn1")).
      andReturn(Correlation(null, abPair, "id1", Map(), Map(), JUL_8_2010_2, timestamp, null, "vsn1", "vsn1", false))
    listener.onMismatch(VersionID(abPair, "id1"), JUL_8_2010_2, null, "vsn1", LiveWindow); expectLastCall
    replayAll

    policy.onChange(writer, DownstreamPairChangeEvent(VersionID(abPair, "id1"), attributes, JUL_8_2010_2, "vsn1"))
    verifyAll
  }

  protected def shouldStoreDownstreamCorrelatedChangesToCorrelationStoreAndNotifySessionManager(
    upstreamCategories: Map[String, CategoryDescriptor],
    downstreamCategories: Map[String, CategoryDescriptor],
    attributes: Seq[String],
    downstreamAttributes: Map[String, TypedAttribute]
  ) {
    pair.upstream.categories = upstreamCategories
    pair.downstream.categories = downstreamCategories
    val timestamp = new DateTime
    expect(writer.storeDownstreamVersion(VersionID(abPair, "id1"), downstreamAttributes, JUL_8_2010_2, "vsn1", "vsn2")).
      andReturn(Correlation(null, abPair, "id1", null, toStrMap(downstreamAttributes), JUL_8_2010_2, timestamp, null, "vsn1", "vsn1", false))
    listener.onMismatch(VersionID(abPair, "id1"), JUL_8_2010_2, null, "vsn1", LiveWindow); expectLastCall
    replayAll

    policy.onChange(writer, DownstreamCorrelatedPairChangeEvent(VersionID(abPair, "id1"), attributes, JUL_8_2010_2, "vsn1", "vsn2"))
    verifyAll
  }

  protected def shouldRaiseMatchEventWhenDownstreamCausesMatchOfUpstream(
    upstreamCategories: Map[String, CategoryDescriptor],
    downstreamCategories: Map[String, CategoryDescriptor],
    attributes: Seq[String],
    downstreamAttributes: Map[String, TypedAttribute]
  ) {
    pair.upstream.categories = upstreamCategories
    pair.downstream.categories = downstreamCategories
    val timestamp = new DateTime
    expect(writer.storeDownstreamVersion(VersionID(abPair, "id1"), downstreamAttributes, JUL_8_2010_2, "vsn1", "vsn2")).
      andReturn(Correlation(null, abPair, "id1", null, toStrMap(downstreamAttributes), JUL_8_2010_2, timestamp, "vsn1", "vsn1", "vsn2", true))
    listener.onMatch(VersionID(abPair, "id1"), "vsn1", LiveWindow); expectLastCall
    replayAll

    policy.onChange(writer, DownstreamCorrelatedPairChangeEvent(VersionID(abPair, "id1"), attributes, JUL_8_2010_2, "vsn1", "vsn2"))
    verifyAll
  }

  //
  // Standard Types
  //

  protected case class UpstreamVersion(id:VersionID, attributes:Map[String, String], lastUpdate:DateTime, vsn:String)
  protected case class UpstreamVersionAnswer(hs:Seq[UpstreamVersion]) extends IAnswer[Unit] {
    def answer {
      val args = EasyMock.getCurrentArguments
      val cb = args(1).asInstanceOf[Function4[VersionID, Map[String, String], DateTime, String, Unit]]

      hs.foreach { case UpstreamVersion(id, attributes, lastUpdate, vsn) =>
        cb(id, attributes, lastUpdate, vsn)
      }
    }
  }
  protected case class DownstreamVersion(id:VersionID, attributes:Map[String, String], lastUpdate:DateTime, usvn:String, dsvn:String)
  protected case class DownstreamVersionAnswer(hs:Seq[DownstreamVersion]) extends IAnswer[Unit] {
    def answer {
      val args = EasyMock.getCurrentArguments
      val cb = args(1).asInstanceOf[Function5[VersionID, Map[String, String], DateTime, String, String, Unit]]

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
    store.queryUpstreams(EasyMock.eq(constraints), anyUnitF4)
      expectLastCall[Unit].andAnswer(UpstreamVersionAnswer(storeResp))
  }

  protected def expectDownstreamAggregateSync(bucketing:Map[String, CategoryFunction], constraints: Seq[QueryConstraint], partResp: Seq[AggregateDigest],
                                              storeResp: Seq[DownstreamVersion]) {
    expectDownstreamAggregateSync(abPair, bucketing, constraints, partResp, storeResp)
  }

  protected def expectDownstreamAggregateSync(pair:String, bucketing:Map[String, CategoryFunction], constraints:Seq[QueryConstraint], partResp:Seq[AggregateDigest], storeResp:Seq[DownstreamVersion]) {
    expect(dsMock.queryAggregateDigests(bucketing, constraints)).andReturn(partResp)
    store.queryDownstreams(EasyMock.eq(constraints), anyUnitF5)
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

    expect(store.queryUpstreams(EasyMock.eq(constraints))).andReturn(correlations)
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

    expect(store.queryDownstreams(EasyMock.eq(constraints))).andReturn(correlations)
  }

  protected def toStrMap(attrs:Map[String, TypedAttribute]) = attrs.map { case (k, v) => k -> v.value }.toMap
}