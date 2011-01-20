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
import net.lshift.diffa.kernel.config.ConfigStore
import net.lshift.diffa.kernel.config.Pair
import net.lshift.diffa.kernel.util.Conversions._
import org.joda.time.DateTime
import org.easymock.{IAnswer, EasyMock}
import net.lshift.diffa.kernel.events.VersionID

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

  val store = createStrictMock("versionStore", classOf[VersionCorrelationStore])
  EasyMock.checkOrder(store, false)   // Store doesn't care about order
  val listener = createStrictMock("listener", classOf[DifferencingListener])

  val configStore = createStrictMock("configStore", classOf[ConfigStore])

  protected def replayAll = replay(configStore, usMock, dsMock, store, listener)
  protected def verifyAll = verify(configStore, usMock, dsMock, store, listener, configStore)

  /**
   * Run the scenario with the top levels matching. The policy should not progress any further than the top level.
   */
  @Theory
  def shouldStopAtTopLevelWhenTopLevelBucketsMatch(scenario:Scenario) {
    setupStubs(scenario)

    expectUpstreamAggregateSync(scenario.pair, scenario.tx.bucketing, scenario.tx.constraints, scenario.tx.respBuckets, scenario.tx.respBuckets)
    expectDownstreamAggregateSync(scenario.pair, scenario.tx.bucketing, scenario.tx.constraints, scenario.tx.respBuckets, scenario.tx.respBuckets)

    // We should still see an unmatched version check
    expect(store.unmatchedVersions(EasyMock.eq(scenario.pair.key), EasyMock.eq(scenario.tx.constraints))).andReturn(Seq())
    replayAll

    policy.difference(scenario.pair.key, usMock, dsMock, nullListener)
    verifyAll
  }

  /**
   * Run the scenario with the store not any content for either half. Policy should run top-level, then jump directly
   * to the individual level.
   */
  @Theory
  def shouldJumpToLowestLevelsStraightAfterTopWhenStoreIsEmpty(scenario:Scenario) {
    setupStubs(scenario)

    expectUpstreamAggregateSync(scenario.pair, scenario.tx.bucketing, scenario.tx.constraints, scenario.tx.respBuckets, Seq())
    scenario.tx.respBuckets.foreach(b => {
      expectUpstreamEntitySync(scenario.pair, b.nextTx.constraints, b.allVsns, Seq())
      expectUpstreamEntityStore(scenario.pair, b.allVsns)
    })

    expectDownstreamAggregateSync(scenario.pair, scenario.tx.bucketing, scenario.tx.constraints, scenario.tx.respBuckets, Seq())
    scenario.tx.respBuckets.foreach(b => {
      expectDownstreamEntitySync(scenario.pair, b.nextTx.constraints, b.allVsns, Seq())
      expectDownstreamEntityStore(scenario.pair, b.allVsns)
    })

    // We should still see an unmatched version check
    expect(store.unmatchedVersions(EasyMock.eq(scenario.pair.key), EasyMock.eq(scenario.tx.constraints))).andReturn(Seq())
    replayAll

    policy.difference(scenario.pair.key, usMock, dsMock, nullListener)
    verifyAll
  }


  //
  // Helpers
  //

  protected def setupStubs(scenario:Scenario) {
    expect(configStore.getPair(scenario.pair.key)).andReturn(scenario.pair).anyTimes
  }

  protected def expectUpstreamAggregateSync(pair:Pair, bucketing:Map[String, CategoryFunction], constraints:Seq[QueryConstraint],
                                            partResp:Seq[Bucket], storeResp:Seq[Bucket]) {
    expect(usMock.queryAggregateDigests(bucketing, constraints)).andReturn(participantDigestResponse(partResp))
    store.queryUpstreams(EasyMock.eq(pair.key), EasyMock.eq(constraints), anyUnitF4)
      expectLastCall[Unit].andAnswer(UpstreamVersionAnswer(pair, storeResp))
  }
  protected def expectDownstreamAggregateSync(pair:Pair, bucketing:Map[String, CategoryFunction], constraints:Seq[QueryConstraint],
                                              partResp:Seq[Bucket], storeResp:Seq[Bucket]) {
    expect(dsMock.queryAggregateDigests(bucketing, constraints)).andReturn(participantDigestResponse(partResp))
    store.queryDownstreams(EasyMock.eq(pair.key), EasyMock.eq(constraints), anyUnitF5)
      expectLastCall[Unit].andAnswer(DownstreamVersionAnswer(pair, storeResp))
  }

  protected def expectUpstreamEntitySync(pair:Pair, constraints:Seq[QueryConstraint], partResp:Seq[Vsn], storeResp:Seq[Vsn]) {
    expect(usMock.queryEntityVersions(constraints)).andReturn(participantEntityResponse(partResp))
    val correlations = storeResp.map(v=> {
      Correlation(id = v.id, upstreamAttributes = v.strAttrs, lastUpdate = v.lastUpdated, upstreamVsn = v.vsn)
    })

    expect(store.queryUpstreams(EasyMock.eq(pair.key), EasyMock.eq(constraints))).andReturn(correlations)
  }
  protected def expectDownstreamEntitySync(pair:Pair, constraints:Seq[QueryConstraint], partResp:Seq[Vsn], storeResp:Seq[Vsn]) {
    expect(dsMock.queryEntityVersions(constraints)).andReturn(participantEntityResponse(partResp))
    val correlations = storeResp.map(v=> {
      Correlation(id = v.id, downstreamAttributes = v.strAttrs, lastUpdate = v.lastUpdated, downstreamDVsn = v.vsn)
    })

    expect(store.queryDownstreams(EasyMock.eq(pair.key), EasyMock.eq(constraints))).andReturn(correlations)
  }

  protected def expectUpstreamEntityStore(pair:Pair, entities:Seq[Vsn]) {
    entities.foreach(v => {
      expect(store.storeUpstreamVersion(VersionID(pair.key, v.id), v.strAttrs, v.lastUpdated, v.vsn)).
        andReturn(Correlation(null, pair.key, v.id, v.strAttrs, null, v.lastUpdated, new DateTime, v.vsn, null, null))
    })
  }
  protected def expectDownstreamEntityStore(pair:Pair, entities:Seq[Vsn]) {
    entities.foreach(v => {
      expect(store.storeDownstreamVersion(VersionID(pair.key, v.id), v.strAttrs, v.lastUpdated, v.vsn, v.vsn)).
        andReturn(Correlation(null, pair.key, v.id, null, v.strAttrs, v.lastUpdated, new DateTime, null, v.vsn, v.vsn))
    })
  }

  protected def participantDigestResponse(buckets:Seq[Bucket]):Seq[AggregateDigest] =
    buckets.map(b => AggregateDigest(AttributesUtil.toSeq(b.attrs), new DateTime, b.vsn))
  protected def participantEntityResponse(entities:Seq[Vsn]):Seq[EntityVersion] =
    entities.map(e => EntityVersion(e.id, AttributesUtil.toSeq(e.strAttrs), e.lastUpdated, e.vsn))

  protected abstract class VersionAnswer[T] extends IAnswer[Unit] {
    def res:Seq[Bucket]

    def answer {
      val args = EasyMock.getCurrentArguments
      val cb = args(2).asInstanceOf[T]

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
}
object AbstractDataDrivenPolicyTest {

  //
  // Scenarios
  //

  @DataPoint def datesOnlyScenario = Scenario(
    Pair(key = "ab", categories = Map("bizDate" -> "date")),
    AggregateTx(Map("bizDate" -> yearly), Seq(unbounded("bizDate")), Seq(
      Bucket("2010", Map("bizDate" -> "2010"),
        AggregateTx(Map("bizDate" -> monthly), Seq(range("bizDate", START_2010, END_2010)), Seq(
          Bucket("2010-07", Map("bizDate" -> "2010-07"),
            AggregateTx(Map("bizDate" -> daily), Seq(range("bizDate", JUL_2010, END_JUL_2010)), Seq(
              Bucket("2010-07-08", Map("bizDate" -> "2010-07-08"),
                EntityTx(Seq(range("bizDate", JUL_8_2010, END_JUL_8_2010)), Seq(
                  Vsn("id1", Map("bizDate" -> JUL_8_2010_1), "vsn1")
                ))),
              Bucket("2010-07-09", Map("bizDate" -> "2010-07-09"),
                EntityTx(Seq(range("bizDate", JUL_9_2010, END_JUL_9_2010)), Seq(
                  Vsn("id2", Map("bizDate" -> JUL_8_2010_2), "vsn2")
                )))
            ))),
          Bucket("2010-08", Map("bizDate" -> "2010-08"),
            AggregateTx(Map("bizDate" -> daily), Seq(range("bizDate", AUG_2010, END_AUG_2010)), Seq(
              Bucket("2010-08-02", Map("bizDate" -> "2010-08-02"),
                EntityTx(Seq(range("bizDate", AUG_11_2010, END_AUG_11_2010)), Seq(
                  Vsn("id3", Map("bizDate" -> AUG_11_2010_1), "vsn3")
                )))
            )))
        ))),
      Bucket("2011", Map("bizDate" -> "2011"),
        AggregateTx(Map("bizDate" -> monthly), Seq(range("bizDate", START_2011, END_2011)), Seq(
          Bucket("2011-01", Map("bizDate" -> "2011-01"),
            AggregateTx(Map("bizDate" -> daily), Seq(range("bizDate", JAN_2011, END_JAN_2011)), Seq(
              Bucket("2011-01-20", Map("bizDate" -> "2011-01-20"),
                EntityTx(Seq(range("bizDate", JAN_20_2011, END_JAN_20_2011)), Seq(
                  Vsn("id4", Map("bizDate" -> JAN_20_2011_1), "vsn4")
                )))
            )))
        )))
    )))

  @DataPoint def integersOnlyScenario = Scenario(
    Pair(key = "bc", categories = Map("someInt" -> "int")),
    AggregateTx(Map("someInt" -> thousands), Seq(unbounded("someInt")), Seq(
      Bucket("1000", Map("someInt" -> "1000"),
        AggregateTx(Map("someInt" -> hundreds), Seq(range("someInt", 1000, 1999)), Seq(
          Bucket("1200", Map("someInt" -> "1200"),
            AggregateTx(Map("someInt" -> tens), Seq(range("someInt", 1200, 1299)), Seq(
              Bucket("1230", Map("someInt" -> "1230"),
                EntityTx(Seq(range("someInt", 1230, 1239)), Seq(
                  Vsn("id1", Map("someInt" -> 1234), "vsn1")
                ))),
              Bucket("1240", Map("someInt" -> "1240"),
                EntityTx(Seq(range("someInt", 1240, 1249)), Seq(
                  Vsn("id2", Map("someInt" -> 1245), "vsn2")
                )))
            ))),
          Bucket("1300", Map("someInt" -> "1300"),
            AggregateTx(Map("someInt" -> tens), Seq(range("someInt", 1300, 1399)), Seq(
              Bucket("1350", Map("someInt" -> "1350"),
                EntityTx(Seq(range("someInt", 1350, 1359)), Seq(
                  Vsn("id3", Map("someInt" -> 1357), "vsn3")
                )))
            )))
        ))),
      Bucket("2000", Map("someInt" -> "2000"),
        AggregateTx(Map("someInt" -> hundreds), Seq(range("someInt", 2000, 2999)), Seq(
          Bucket("2300", Map("someInt" -> "2300"),
            AggregateTx(Map("someInt" -> tens), Seq(range("someInt", 2300, 2399)), Seq(
              Bucket("2340", Map("someInt" -> "2340"),
                EntityTx(Seq(range("someInt", 2340, 2349)), Seq(
                  Vsn("id4", Map("someInt" -> 2345), "vsn4")
                )))
            )))
        )))
    )))

  
  //
  // Aliases
  //

  val yearly = YearlyCategoryFunction
  val monthly = MonthlyCategoryFunction
  val daily = DailyCategoryFunction
  val individual = IndividualCategoryFunction

  val thousands = AutoNarrowingIntegerCategoryFunction(1000, 10)
  val hundreds = AutoNarrowingIntegerCategoryFunction(100, 10)
  val tens = AutoNarrowingIntegerCategoryFunction(10, 10)

  def unbounded(n:String) = UnboundedRangeQueryConstraint(n)
  def range(n:String, lower:Any, upper:Any) = RangeQueryConstraint(n, Seq(lower.toString, upper.toString))


  //
  // Type Definitions
  //

  case class Scenario(pair:Pair, tx:AggregateTx)

  abstract class Tx { def constraints:Seq[QueryConstraint]; def allVsns:Seq[Vsn] }
  case class AggregateTx(bucketing:Map[String, CategoryFunction], constraints:Seq[QueryConstraint], respBuckets:Seq[Bucket]) extends Tx {
    lazy val allVsns = respBuckets.flatMap(b => b.allVsns)
  }
  case class EntityTx(constraints:Seq[QueryConstraint], entities:Seq[Vsn]) extends Tx {
    lazy val allVsns = entities
  }

  case class Bucket(name:String, attrs:Map[String, String], nextTx:Tx) {
    lazy val allVsns = nextTx.allVsns
    lazy val vsn = DigestUtils.md5Hex(allVsns.map(v => v.vsn).foldLeft("")(_ + _))
  }
  case class Vsn(id:String, attrs:Map[String, Any], vsn:String) {
    def strAttrs = attrs.map { case (k, v) => k -> v.toString }.toMap
    lazy val lastUpdated = new DateTime
  }
}