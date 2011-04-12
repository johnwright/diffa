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

import scala.collection.JavaConversions._
import org.easymock.EasyMock._
import net.lshift.diffa.kernel.util.EasyMockScalaUtils._
import org.apache.commons.codec.digest.DigestUtils
import net.lshift.diffa.kernel.participants._
import net.lshift.diffa.kernel.participants.IntegerCategoryFunction._
import org.junit.runner.RunWith
import net.lshift.diffa.kernel.util.Dates._
import org.junit.experimental.theories.{Theory, Theories, DataPoint}
import org.joda.time.DateTime
import org.easymock.{IAnswer, EasyMock}
import net.lshift.diffa.kernel.events.VersionID
import net.lshift.diffa.kernel.config.CategoryDescriptor._
import net.lshift.diffa.kernel.config._

/**
 * Framework and scenario definitions for data-driven policy tests.
 */
@RunWith(classOf[Theories])
abstract class AbstractDataDrivenPolicyTest {
  import AbstractDataDrivenPolicyTest._

  // The policy instance under test
  protected def policy:VersionPolicy

  // The various mocks for listeners and participants
  val usMock = createStrictMock("us", classOf[UpstreamParticipant])
  val dsMock = createStrictMock("ds", classOf[DownstreamParticipant])
  EasyMock.checkOrder(usMock, false)   // Not all participant operations are going to be strictly ordered
  EasyMock.checkOrder(dsMock, false)   // Not all participant operations are going to be strictly ordered

  val nullListener = new NullDifferencingListener

  val writer = createMock("writer", classOf[VersionCorrelationWriter])
  val store = createMock("versionStore", classOf[VersionCorrelationStore])
  val stores = new VersionCorrelationStoreFactory {
    def apply(pairKey: String) = store
    def remove(pairKey: String) {}
    def close {}
  }
  val listener = createStrictMock("listener", classOf[DifferencingListener])

  val configStore = createStrictMock("configStore", classOf[ConfigStore])

  protected def replayAll = replay(configStore, usMock, dsMock, store, writer, listener)
  protected def verifyAll = verify(configStore, usMock, dsMock, store, writer, listener, configStore)

  /**
   * Scenario with the top levels matching. The policy should not progress any further than the top level.
   */
  @Theory
  def shouldStopAtTopLevelWhenTopLevelBucketsMatch(scenario:Scenario) {
    setupStubs(scenario)

    scenario.tx.foreach { tx =>
      expectUpstreamAggregateSync(scenario.pair, tx.bucketing, tx.constraints, tx.respBuckets, tx.respBuckets)
      expectDownstreamAggregateSync(scenario.pair, tx.bucketing, tx.constraints, tx.respBuckets, tx.respBuckets)
    }

    // Expect to see the writer flushed
    writer.flush; expectLastCall.once

    // We should still see an unmatched version check
    expectUnmatchedVersionCheck(scenario)

    replayAll

    policy.difference(scenario.pair.key, writer, usMock, dsMock, nullListener)
    verifyAll
  }

  /**
   * Scenario with the store not any content for either half. Policy should run top-level, then jump directly
   * to the individual level.
   */
  @Theory
  def shouldJumpToLowestLevelsStraightAfterTopWhenStoreIsEmpty(scenario:Scenario) {
    setupStubs(scenario)

    scenario.tx.foreach { tx =>
      expectUpstreamAggregateSync(scenario.pair, tx.bucketing, tx.constraints, tx.respBuckets, Seq())
      tx.respBuckets.foreach(b => {
        expectUpstreamEntitySync(scenario.pair, b.nextTx.constraints, b.allVsns, Seq())
        expectUpstreamEntityStore(scenario.pair, b.allVsns, false)
      })

      expectDownstreamAggregateSync(scenario.pair, tx.bucketing, tx.constraints, tx.respBuckets, Seq())
      tx.respBuckets.foreach(b => {
        expectDownstreamEntitySync(scenario.pair, b.nextTx.constraints, b.allVsns, Seq())
        expectDownstreamEntityStore(scenario.pair, b.allVsns, false)
      })
    }

    // Expect to see the writer flushed
    writer.flush; expectLastCall.once

    // We should still see an unmatched version check
    expectUnmatchedVersionCheck(scenario)

    replayAll

    policy.difference(scenario.pair.key, writer, usMock, dsMock, nullListener)
    verifyAll
  }

  /**
   * Scenario with the store being out-of-date for a upstream leaf-node.
   */
  @Theory
  def shouldCorrectOutOfDateUpstreamEntity(scenario:Scenario) {
    setupStubs(scenario)

    scenario.tx.foreach { tx =>
      // Alter the version of the first entity in the upstream tree, then expect traversal to it
      val updated = tx.alterFirstVsn("newVsn1")

      traverseFirstBranch(updated, tx) {
        case (tx1:AggregateTx, tx2:AggregateTx) =>
          expectUpstreamAggregateSync(scenario.pair, tx1.bucketing, tx1.constraints, tx1.respBuckets, tx2.respBuckets)
        case (tx1:EntityTx, tx2:EntityTx) =>
          expectUpstreamEntitySync(scenario.pair, tx1.constraints, tx1.entities, tx2.entities)
      }
      expectUpstreamEntityStore(scenario.pair, Seq(updated.firstVsn), true)

      // Expect to see an event about the version being matched (since we told the datastore to report it as matched)
      listener.onMatch(VersionID(scenario.pair.key, updated.firstVsn.id), updated.firstVsn.vsn)

      // Expect only a top-level sync on the downstream
      expectDownstreamAggregateSync(scenario.pair, tx.bucketing, tx.constraints, tx.respBuckets, tx.respBuckets)
    }

    // Expect to see the writer flushed
    writer.flush; expectLastCall.once

    // We should still see an unmatched version check
    expectUnmatchedVersionCheck(scenario)

    replayAll

    policy.difference(scenario.pair.key, writer, usMock, dsMock, nullListener)
    verifyAll
  }

  /**
   * Scenario with the store being out-of-date for a downstream leaf-node.
   */
  @Theory
  def shouldCorrectOutOfDateDownstreamEntity(scenario:Scenario) {
    setupStubs(scenario)

    scenario.tx.foreach { tx =>
      // Expect only a top-level sync on the upstream
      expectUpstreamAggregateSync(scenario.pair, tx.bucketing, tx.constraints, tx.respBuckets, tx.respBuckets)

      // Alter the version of the first entity in the downstream tree, then expect traversal to it
      val updated = tx.alterFirstVsn("newVsn1")
      traverseFirstBranch(updated, tx) {
        case (tx1:AggregateTx, tx2:AggregateTx) =>
          expectDownstreamAggregateSync(scenario.pair, tx1.bucketing, tx1.constraints, tx1.respBuckets, tx2.respBuckets)
        case (tx1:EntityTx, tx2:EntityTx) =>
          expectDownstreamEntitySync(scenario.pair, tx1.constraints, tx1.entities, tx2.entities)
      }
      expectDownstreamEntityStore(scenario.pair, Seq(updated.firstVsn), true)

      // Expect to see an event about the version being matched (since we told the datastore to report it as matched)
      listener.onMatch(VersionID(scenario.pair.key, updated.firstVsn.id), updated.firstVsn.vsn)
    }

    // Expect to see the writer flushed
    writer.flush; expectLastCall.once

    // We should still see an unmatched version check
    expectUnmatchedVersionCheck(scenario)

    replayAll

    policy.difference(scenario.pair.key, writer, usMock, dsMock, nullListener)
    verifyAll
  }


  //
  // Helpers
  //

  protected def setupStubs(scenario:Scenario) {
    expect(configStore.getPair(scenario.pair.key)).andReturn(scenario.pair).anyTimes
  }

  protected def expectUnmatchedVersionCheck(scenario:Scenario) = {
    val us = scenario.pair.upstream.defaultConstraints
    val ds = scenario.pair.downstream.defaultConstraints
    expect(store.unmatchedVersions(EasyMock.eq(us), EasyMock.eq(ds))).andReturn(Seq())
  }

  protected def expectUpstreamAggregateSync(pair:Pair, bucketing:Map[String, CategoryFunction], constraints:Seq[QueryConstraint],
                                            partResp:Seq[Bucket], storeResp:Seq[Bucket]) {
    expect(usMock.queryAggregateDigests(bucketing, constraints)).andReturn(participantDigestResponse(partResp))
    store.queryUpstreams(EasyMock.eq(constraints), anyUnitF4)
      expectLastCall[Unit].andAnswer(UpstreamVersionAnswer(pair, storeResp))
  }
  protected def expectDownstreamAggregateSync(pair:Pair, bucketing:Map[String, CategoryFunction], constraints:Seq[QueryConstraint],
                                              partResp:Seq[Bucket], storeResp:Seq[Bucket]) {
    expect(dsMock.queryAggregateDigests(bucketing, constraints)).andReturn(participantDigestResponse(partResp))
    store.queryDownstreams(EasyMock.eq(constraints), anyUnitF5)
      expectLastCall[Unit].andAnswer(DownstreamVersionAnswer(pair, storeResp))
  }

  protected def expectUpstreamEntitySync(pair:Pair, constraints:Seq[QueryConstraint], partResp:Seq[Vsn], storeResp:Seq[Vsn]) {
    expect(usMock.queryEntityVersions(constraints)).andReturn(participantEntityResponse(partResp))
    val correlations = storeResp.map(v=> {
      Correlation(id = v.id, upstreamAttributes = v.strAttrs, lastUpdate = v.lastUpdated, upstreamVsn = v.vsn)
    })

    expect(store.queryUpstreams(EasyMock.eq(constraints))).andReturn(correlations)
  }
  protected def expectDownstreamEntitySync(pair:Pair, constraints:Seq[QueryConstraint], partResp:Seq[Vsn], storeResp:Seq[Vsn]) {
    expect(dsMock.queryEntityVersions(constraints)).andReturn(participantEntityResponse(partResp))
    val correlations = storeResp.map(v=> {
      Correlation(id = v.id, downstreamAttributes = v.strAttrs, lastUpdate = v.lastUpdated, downstreamDVsn = v.vsn)
    })

    expect(store.queryDownstreams(EasyMock.eq(constraints))).andReturn(correlations)
  }

  protected def expectUpstreamEntityStore(pair:Pair, entities:Seq[Vsn], matched:Boolean) {
    entities.foreach(v => {
      val downstreamVsnToUse = if (matched) { v.vsn } else { null }   // If we're matched, make the vsn match

      expect(writer.storeUpstreamVersion(VersionID(pair.key, v.id), v.typedAttrs, v.lastUpdated, v.vsn)).
        andReturn(Correlation(null, pair.key, v.id, v.strAttrs, null, v.lastUpdated, new DateTime, v.vsn, downstreamVsnToUse, downstreamVsnToUse, matched))
    })
  }
  protected def expectDownstreamEntityStore(pair:Pair, entities:Seq[Vsn], matched:Boolean) {
    entities.foreach(v => {
      val upstreamVsnToUse = if (matched) { v.vsn } else { null }   // If we're matched, make the vsn match

      expect(writer.storeDownstreamVersion(VersionID(pair.key, v.id), v.typedAttrs, v.lastUpdated, v.vsn, v.vsn)).
        andReturn(Correlation(null, pair.key, v.id, null, v.strAttrs, v.lastUpdated, new DateTime, upstreamVsnToUse, v.vsn, v.vsn, matched))
    })
  }

  protected def participantDigestResponse(buckets:Seq[Bucket]):Seq[AggregateDigest] =
    buckets.map(b => AggregateDigest(AttributesUtil.toSeq(b.attrs), b.vsn))
  protected def participantEntityResponse(entities:Seq[Vsn]):Seq[EntityVersion] =
    entities.map(e => EntityVersion(e.id, AttributesUtil.toSeq(e.strAttrs), e.lastUpdated, e.vsn))

  protected abstract class VersionAnswer[T] extends IAnswer[Unit] {
    def res:Seq[Bucket]

    def answer {
      val args = EasyMock.getCurrentArguments
      val cb = args(1).asInstanceOf[T]

      // Answer with entities from each bucket's children
      answerEntities(res.flatMap(b => b.allVsns), cb)
    }

    def answerEntities(entities:Seq[Vsn], cb:T):Unit
  }

  protected case class UpstreamVersionAnswer(pair:Pair, res:Seq[Bucket])
      extends VersionAnswer[Function4[VersionID, Map[String, String], DateTime, String, Unit]] {
    def answerEntities(entities:Seq[Vsn], cb:Function4[VersionID, Map[String, String], DateTime, String, Unit]) {
      entities.foreach(v => cb(VersionID(pair.key, v.id), v.strAttrs, v.lastUpdated, v.vsn))
    }
  }
  protected case class DownstreamVersionAnswer(pair:Pair, res:Seq[Bucket])
      extends VersionAnswer[Function5[VersionID, Map[String, String], DateTime, String, String, Unit]] {
    def answerEntities(entities:Seq[Vsn], cb:Function5[VersionID, Map[String, String], DateTime, String, String, Unit]) {
      entities.foreach(v => cb(VersionID(pair.key, v.id), v.strAttrs, v.lastUpdated, v.vsn, v.vsn))
    }
  }

  def traverseFirstBranch(tx1:Tx, tx2:Tx)(cb:((Tx, Tx) => Unit)) {
      cb(tx1, tx2)

      (tx1, tx2) match {
        case (atx1:AggregateTx, atx2:AggregateTx) => traverseFirstBranch(atx1.respBuckets(0).nextTx, atx2.respBuckets(0).nextTx)(cb)
        case (atx1:AggregateTx, _) => traverseFirstBranch(atx1.respBuckets(0).nextTx, null)(cb)
        case (_, atx2:AggregateTx) => traverseFirstBranch(null, atx2.respBuckets(0).nextTx)(cb)
        case _ => 
      }
    }
}
object AbstractDataDrivenPolicyTest {

  //
  // Scenarios
  //

  val dateCategoryDescriptor = new RangeCategoryDescriptor("date")
  val intCategoryDescriptor = new RangeCategoryDescriptor("int")
  val stringCategoryDescriptor = new PrefixCategoryDescriptor(1, 3, 1)

  /**
   * As part of #203, elements of a set are sent out individually by default.
   * For the sake of simplicity, the old behaviour (to send them out as a batch) can not be configured.
   * Should any body ask for this, this behavior be may re-instated at some point.
   */
  @DataPoint def setOnlyScenario = Scenario(
    Pair(key = "ab",
      upstream = new Endpoint(categories = Map("someString" -> new SetCategoryDescriptor(Set("A","B","C")))),
      downstream = new Endpoint(categories = Map("someString" -> new SetCategoryDescriptor(Set("A","B","C"))))),
      AggregateTx(Map("someString" -> byName), Seq(SetQueryConstraint("someString",Set("A"))),
        Bucket("A", Map("someString" -> "A"),
          EntityTx(Seq(SetQueryConstraint("someString", Set("A"))),
            Vsn("id1", Map("someString" -> "A"), "vsn1"),
            Vsn("id2", Map("someString" -> "A"), "vsn2")
          )
        )
      ),
      AggregateTx(Map("someString" -> byName), Seq(SetQueryConstraint("someString",Set("B"))),
        Bucket("B", Map("someString" -> "B"),
          EntityTx(Seq(SetQueryConstraint("someString", Set("B"))),
            Vsn("id3", Map("someString" -> "B"), "vsn3"),
            Vsn("id4", Map("someString" -> "B"), "vsn4")
          )
        )
      ),
      AggregateTx(Map("someString" -> byName), Seq(SetQueryConstraint("someString",Set("C"))),
        Bucket("C", Map("someString" -> "C"),
          EntityTx(Seq(SetQueryConstraint("someString", Set("C"))),
            Vsn("id5", Map("someString" -> "C"), "vsn5"),
            Vsn("id6", Map("someString" -> "C"), "vsn6")
          )
        )
      )
    )

  @DataPoint def datesOnlyScenario = Scenario(
    Pair(key = "ab",
      upstream = new Endpoint(categories = Map("bizDate" -> dateCategoryDescriptor)),
      downstream = new Endpoint(categories = Map("bizDate" -> dateCategoryDescriptor))),
    AggregateTx(Map("bizDate" -> yearly), Seq(unbounded("bizDate")),
      Bucket("2010", Map("bizDate" -> "2010"),
        AggregateTx(Map("bizDate" -> monthly), Seq(dateRange("bizDate", START_2010, END_2010)),
          Bucket("2010-07", Map("bizDate" -> "2010-07"),
            AggregateTx(Map("bizDate" -> daily), Seq(dateRange("bizDate", JUL_2010, END_JUL_2010)),
              Bucket("2010-07-08", Map("bizDate" -> "2010-07-08"),
                EntityTx(Seq(dateRange("bizDate", JUL_8_2010, END_JUL_8_2010)),
                  Vsn("id1", Map("bizDate" -> JUL_8_2010_1), "vsn1"),
                  Vsn("id2", Map("bizDate" -> JUL_8_2010_2), "vsn2")
                )),
              Bucket("2010-07-09", Map("bizDate" -> "2010-07-09"),
                EntityTx(Seq(dateRange("bizDate", JUL_9_2010, END_JUL_9_2010)),
                  Vsn("id3", Map("bizDate" -> JUL_9_2010_1), "vsn3")
                ))
            )),
          Bucket("2010-08", Map("bizDate" -> "2010-08"),
            AggregateTx(Map("bizDate" -> daily), Seq(dateRange("bizDate", AUG_2010, END_AUG_2010)),
              Bucket("2010-08-02", Map("bizDate" -> "2010-08-02"),
                EntityTx(Seq(dateRange("bizDate", AUG_11_2010, END_AUG_11_2010)),
                  Vsn("id4", Map("bizDate" -> AUG_11_2010_1), "vsn4")
                ))
            ))
        )),
      Bucket("2011", Map("bizDate" -> "2011"),
        AggregateTx(Map("bizDate" -> monthly), Seq(dateRange("bizDate", START_2011, END_2011)),
          Bucket("2011-01", Map("bizDate" -> "2011-01"),
            AggregateTx(Map("bizDate" -> daily), Seq(dateRange("bizDate", JAN_2011, END_JAN_2011)),
              Bucket("2011-01-20", Map("bizDate" -> "2011-01-20"),
                EntityTx(Seq(dateRange("bizDate", JAN_20_2011, END_JAN_20_2011)),
                  Vsn("id5", Map("bizDate" -> JAN_20_2011_1), "vsn5")
                ))
            ))
        ))
    ))

  @DataPoint def integersOnlyScenario = Scenario(
    Pair(key = "bc",
      upstream = new Endpoint(categories = Map("someInt" -> intCategoryDescriptor)),
      downstream = new Endpoint(categories = Map("someInt" -> intCategoryDescriptor))),
    AggregateTx(Map("someInt" -> thousands), Seq(unbounded("someInt")),
      Bucket("1000", Map("someInt" -> "1000"),
        AggregateTx(Map("someInt" -> hundreds), Seq(intRange("someInt", 1000, 1999)),
          Bucket("1200", Map("someInt" -> "1200"),
            AggregateTx(Map("someInt" -> tens), Seq(intRange("someInt", 1200, 1299)),
              Bucket("1230", Map("someInt" -> "1230"),
                EntityTx(Seq(intRange("someInt", 1230, 1239)),
                  Vsn("id1", Map("someInt" -> 1234), "vsn1")
                )),
              Bucket("1240", Map("someInt" -> "1240"),
                EntityTx(Seq(intRange("someInt", 1240, 1249)),
                  Vsn("id2", Map("someInt" -> 1245), "vsn2")
                ))
            )),
          Bucket("1300", Map("someInt" -> "1300"),
            AggregateTx(Map("someInt" -> tens), Seq(intRange("someInt", 1300, 1399)),
              Bucket("1350", Map("someInt" -> "1350"),
                EntityTx(Seq(intRange("someInt", 1350, 1359)),
                  Vsn("id3", Map("someInt" -> 1357), "vsn3")
                ))
            ))
        )),
      Bucket("2000", Map("someInt" -> "2000"),
        AggregateTx(Map("someInt" -> hundreds), Seq(intRange("someInt", 2000, 2999)),
          Bucket("2300", Map("someInt" -> "2300"),
            AggregateTx(Map("someInt" -> tens), Seq(intRange("someInt", 2300, 2399)),
              Bucket("2340", Map("someInt" -> "2340"),
                EntityTx(Seq(intRange("someInt", 2340, 2349)),
                  Vsn("id4", Map("someInt" -> 2345), "vsn4")
                ))
            ))
        ))
    ))

  @DataPoint def stringsOnlyScenario = Scenario(
    Pair(key = "bc",
      upstream = new Endpoint(categories = Map("someString" -> stringCategoryDescriptor)),
      downstream = new Endpoint(categories = Map("someString" -> stringCategoryDescriptor))),
    AggregateTx(Map("someString" -> oneCharString), Seq(unbounded("someString")),
      Bucket("A", Map("someString" -> "A"),
        AggregateTx(Map("someString" -> twoCharString), Seq(prefix("someString", "A")),
          Bucket("AB", Map("someString" -> "AB"),
            AggregateTx(Map("someString" -> threeCharString), Seq(prefix("someString", "AB")),
              Bucket("ABC", Map("someString" -> "ABC"),
                EntityTx(Seq(prefix("someString", "ABC")),
                  Vsn("id1", Map("someString" -> "ABC"), "vsn1")
                )),
              Bucket("ABD", Map("someString" -> "ABD"),
                EntityTx(Seq(prefix("someString", "ABD")),
                  Vsn("id2", Map("someString" -> "ABDZ"), "vsn2")
                ))
            )),
          Bucket("AC", Map("someString" -> "AC"),
            AggregateTx(Map("someString" -> threeCharString), Seq(prefix("someString", "AC")),
              Bucket("ACD", Map("someString" -> "ACD"),
                EntityTx(Seq(prefix("someString", "ACD")),
                  Vsn("id3", Map("someString" -> "ACDC"), "vsn3")
                ))
            ))
        )),
      Bucket("Z", Map("someString" -> "Z"),
        AggregateTx(Map("someString" -> twoCharString), Seq(prefix("someString", "Z")),
          Bucket("ZY", Map("someString" -> "ZY"),
            AggregateTx(Map("someString" -> threeCharString), Seq(prefix("someString", "ZY")),
              Bucket("ZYX", Map("someString" -> "ZYX"),
                EntityTx(Seq(prefix("someString", "ZYX")),
                  Vsn("id4", Map("someString" -> "ZYXXY"), "vsn4")
                ))
            ))
        ))
    ))

  @DataPoint def integersAndDatesScenario = Scenario(
    Pair(key = "ab",
      upstream = new Endpoint(categories = Map("bizDate" -> dateCategoryDescriptor, "someInt" -> intCategoryDescriptor)),
      downstream = new Endpoint(categories = Map("bizDate" -> dateCategoryDescriptor, "someInt" -> intCategoryDescriptor))),
    AggregateTx(Map("bizDate" -> yearly, "someInt" -> thousands), Seq(unbounded("bizDate"), unbounded("someInt")),
      Bucket("2010_1000", Map("bizDate" -> "2010", "someInt" -> "1000"),
        AggregateTx(Map("bizDate" -> monthly, "someInt" -> hundreds), Seq(dateRange("bizDate", START_2010, END_2010), intRange("someInt", 1000, 1999)),
          Bucket("2010-07_1200", Map("bizDate" -> "2010-07", "someInt" -> "1200"),
            AggregateTx(Map("bizDate" -> daily, "someInt" -> tens), Seq(dateRange("bizDate", JUL_2010, END_JUL_2010), intRange("someInt", 1200, 1299)),
              Bucket("2010-07-08_1230", Map("bizDate" -> "2010-07-08", "someInt" -> "1230"),
                EntityTx(Seq(dateRange("bizDate", JUL_8_2010, END_JUL_8_2010), intRange("someInt", 1230, 1239)),
                  Vsn("id1", Map("bizDate" -> JUL_8_2010_1, "someInt" -> 1234), "vsn1"),
                  Vsn("id2", Map("bizDate" -> JUL_8_2010_2, "someInt" -> 1235), "vsn2")
                )),
              Bucket("2010-07-09_1240", Map("bizDate" -> "2010-07-09", "someInt" -> "1240"),
                EntityTx(Seq(dateRange("bizDate", JUL_9_2010, END_JUL_9_2010), intRange("someInt", 1240, 1249)),
                  Vsn("id3", Map("bizDate" -> JUL_9_2010_1, "someInt" -> 1245), "vsn3")
                ))
            )),
          Bucket("2010-08_1300", Map("bizDate" -> "2010-08", "someInt" -> "1300"),
            AggregateTx(Map("bizDate" -> daily, "someInt" -> tens), Seq(dateRange("bizDate", AUG_2010, END_AUG_2010), intRange("someInt", 1300, 1399)),
              Bucket("2010-08-02_1350", Map("bizDate" -> "2010-08-02", "someInt" -> "1350"),
                EntityTx(Seq(dateRange("bizDate", AUG_11_2010, END_AUG_11_2010), intRange("someInt", 1350, 1359)),
                  Vsn("id4", Map("bizDate" -> AUG_11_2010_1, "someInt" -> 1357), "vsn4")
                ))
            ))
        )),
      Bucket("2011_2000", Map("bizDate" -> "2011", "someInt" -> "2000"),
        AggregateTx(Map("bizDate" -> monthly, "someInt" -> hundreds), Seq(dateRange("bizDate", START_2011, END_2011), intRange("someInt", 2000, 2999)),
          Bucket("2011-01_2300", Map("bizDate" -> "2011-01", "someInt" -> "2300"),
            AggregateTx(Map("bizDate" -> daily, "someInt" -> tens), Seq(dateRange("bizDate", JAN_2011, END_JAN_2011), intRange("someInt", 2300, 2399)),
              Bucket("2011-01-20_2340", Map("bizDate" -> "2011-01-20", "someInt" -> "2340"),
                EntityTx(Seq(dateRange("bizDate", JAN_20_2011, END_JAN_20_2011), intRange("someInt", 2340, 2349)),
                  Vsn("id5", Map("bizDate" -> JAN_20_2011_1, "someInt" -> 2345), "vsn5")
                ))
            ))
        ))
    ))

  /**
   * As part of #203, elements of a set are sent out individually by default.
   * For the sake of simplicity, the old behaviour (to send them out as a batch) can not be configured.
   * Should any body ask for this, this behavior be may re-instated at some point.
   */

  @DataPoint def setAndDateScenario = Scenario(
    Pair(key = "gh",
      upstream = new Endpoint(categories = Map("bizDate" -> dateCategoryDescriptor, "someString" -> new SetCategoryDescriptor(Set("A","B")))),
      downstream = new Endpoint(categories = Map("bizDate" -> dateCategoryDescriptor, "someString" -> new SetCategoryDescriptor(Set("A","B"))))),
    AggregateTx(Map("bizDate" -> yearly, "someString" -> byName), Seq(unbounded("bizDate"), SetQueryConstraint("someString",Set("A"))),
      Bucket("2010_A", Map("bizDate" -> "2010", "someString" -> "A"),
        AggregateTx(Map("bizDate" -> monthly), Seq(dateRange("bizDate", START_2010, END_2010), SetQueryConstraint("someString",Set("A"))),
          Bucket("2010-07_A", Map("bizDate" -> "2010-07"),
            AggregateTx(Map("bizDate" -> daily), Seq(dateRange("bizDate", JUL_2010, END_JUL_2010), SetQueryConstraint("someString",Set("A"))),
              Bucket("2010-07-08_A", Map("bizDate" -> "2010-07-08"),
                EntityTx(Seq(dateRange("bizDate", JUL_8_2010, END_JUL_8_2010), SetQueryConstraint("someString",Set("A"))),
                  Vsn("id1", Map("bizDate" -> JUL_8_2010_1, "someString" -> "A"), "vsn1"),
                  Vsn("id2", Map("bizDate" -> JUL_8_2010_2, "someString" -> "A"), "vsn2")
                )
              )
            )
          )
        )
      )
    ),
    AggregateTx(Map("bizDate" -> yearly, "someString" -> byName), Seq(unbounded("bizDate"), SetQueryConstraint("someString",Set("B"))),
      Bucket("2011_B", Map("bizDate" -> "2011", "someString" -> "B"),
        AggregateTx(Map("bizDate" -> monthly), Seq(dateRange("bizDate", START_2011, END_2011), SetQueryConstraint("someString",Set("B"))),
          Bucket("2011-01_B", Map("bizDate" -> "2011-01"),
            AggregateTx(Map("bizDate" -> daily), Seq(dateRange("bizDate", JAN_2011, END_JAN_2011), SetQueryConstraint("someString",Set("B"))),
              Bucket("2011-01-20_B", Map("bizDate" -> "2011-01-20"),
                EntityTx(Seq(dateRange("bizDate", JAN_20_2011, END_JAN_20_2011), SetQueryConstraint("someString",Set("B"))),
                  Vsn("id3", Map("bizDate" -> JAN_20_2011_1, "someString" -> "B"), "vsn3")
                )
              )
            )
          )
        )
      )
    )
  )

  //
  // Aliases
  //

  val yearly = YearlyCategoryFunction
  val monthly = MonthlyCategoryFunction
  val daily = DailyCategoryFunction
  val individual = IndividualCategoryFunction

  val byName = ByNameCategoryFunction

  val thousands = AutoNarrowingIntegerCategoryFunction(1000, 10)
  val hundreds = AutoNarrowingIntegerCategoryFunction(100, 10)
  val tens = AutoNarrowingIntegerCategoryFunction(10, 10)

  val oneCharString = StringPrefixCategoryFunction(1, 3, 1)
  val twoCharString = StringPrefixCategoryFunction(2, 3, 1)
  val threeCharString = StringPrefixCategoryFunction(3, 3, 1)

  def unbounded(n:String) = UnboundedRangeQueryConstraint(n)
  def dateRange(n:String, lower:DateTime, upper:DateTime) = DateRangeConstraint(n, lower, upper)
  def intRange(n:String, lower:Int, upper:Int) = IntegerRangeConstraint(n, lower, upper)
  def prefix(n: String, prefix: String) = PrefixQueryConstraint(n, prefix)

  //
  // Type Definitions
  //

  case class Scenario(pair:Pair, tx:AggregateTx*)

  abstract class Tx {
    def constraints:Seq[QueryConstraint]
    def allVsns:Seq[Vsn]
    def alterFirstVsn(newVsn:String):Tx
    def firstVsn:Vsn
    def toString(indent:Int):String
  }

  /**
   * @param bucketing The bucketing policy to apply
   * @param constraints The value constraints being applied to this transaction
   * @param respBuckets The list of buckets expected in this transaction
   */
  case class AggregateTx(bucketing:Map[String, CategoryFunction], constraints:Seq[QueryConstraint], respBuckets:Bucket*) extends Tx {
    lazy val allVsns = respBuckets.flatMap(b => b.allVsns)

    def alterFirstVsn(newVsn:String) =
      // This uses the prepend operator +: to alter the first the element of the list and then re-attach the remainder to create a new sequence
      AggregateTx(bucketing, constraints, (respBuckets(0).alterFirstVsn(newVsn) +: respBuckets.drop(1)):_*)
    def firstVsn = respBuckets(0).nextTx.firstVsn

    def toString(indent:Int) = (" " * indent) + "AggregateTx(" + bucketing + ", " + constraints + ")\n" + respBuckets.map(b => b.toString(indent + 2)).foldLeft("")(_ + _)
  }
  case class EntityTx(constraints:Seq[QueryConstraint], entities:Vsn*) extends Tx {
    lazy val allVsns = entities

    def alterFirstVsn(newVsn:String) = EntityTx(constraints, (entities(0).alterVsn(newVsn) +: entities.drop(1)):_*)
    def firstVsn = entities(0)

    def toString(indent:Int) = (" " * indent) + "EntityTx(" + constraints + ")\n" + entities.map(e => e.toString(indent + 2)).foldLeft("")(_ + _)
  }

  case class Bucket(name:String, attrs:Map[String, String], nextTx:Tx) {
    lazy val allVsns = nextTx.allVsns
    lazy val vsn = DigestUtils.md5Hex(allVsns.map(v => v.vsn).foldLeft("")(_ + _))

    def alterFirstVsn(newVsn:String):Bucket = Bucket(name, attrs, nextTx.alterFirstVsn(newVsn))

    def toString(indent:Int) = (" " * indent) + "Bucket(" + name + ", " + attrs + ", " + vsn + ")\n" + nextTx.toString(indent + 2)
  }
  case class Vsn(id:String, attrs:Map[String, Any], vsn:String) {
    def typedAttrs = attrs.map { case (k, v) => k -> toTyped(v) }.toMap
    def strAttrs = attrs.map { case (k, v) => k -> v.toString }.toMap
    lazy val lastUpdated = new DateTime

    def alterVsn(newVsn:String) = {
      Vsn(id, attrs, newVsn)
    }

    def toString(indent:Int) = (" " * indent) + "Vsn(" + id + ", " + attrs + ", " + vsn + ")\n"

    def toTyped(v:Any) = v match {
      case i:Int       => IntegerAttribute(i)
      case dt:DateTime => DateAttribute(dt)
      case _           => StringAttribute(v.toString)
    }
  }
}