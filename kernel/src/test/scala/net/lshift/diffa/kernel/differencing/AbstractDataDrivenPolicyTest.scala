package net.lshift.diffa.kernel.differencing

import scala.collection.JavaConversions._
import org.easymock.EasyMock._
import net.lshift.diffa.kernel.util.EasyMockScalaUtils._
import org.apache.commons.codec.digest.DigestUtils
import net.lshift.diffa.kernel.participants._
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

  /**
   * Run the scenario with the top levels matching. The policy should not progress any further than the top level.
   */
  @Theory
  def shouldStopAtTopLevelWhenTopLevelBucketsMatch(scenario:AggregateTx) {
    expectUpstreamAggregateSync(scenario.bucketing, scenario.constraints, scenario.respBuckets, scenario.respBuckets)
    expectDownstreamAggregateSync(scenario.bucketing, scenario.constraints, scenario.respBuckets, scenario.respBuckets)

    // We should still see an unmatched version check
    expect(store.unmatchedVersions(EasyMock.eq(abPair), EasyMock.eq(scenario.constraints))).andReturn(Seq())
    replayAll

    policy.difference(abPair, usMock, dsMock, nullListener)
    verifyAll
  }

  protected def expectUpstreamAggregateSync(bucketing:Map[String, CategoryFunction], constraints:Seq[QueryConstraint],
                                            partResp:Seq[Bucket], storeResp:Seq[Bucket]) {
    expect(usMock.queryAggregateDigests(bucketing, constraints)).andReturn(participantResponse(partResp))
    store.queryUpstreams(EasyMock.eq(abPair), EasyMock.eq(constraints), anyUnitF4)
      expectLastCall[Unit].andAnswer(UpstreamVersionAnswer(storeResp))
  }
  protected def expectDownstreamAggregateSync(bucketing:Map[String, CategoryFunction], constraints:Seq[QueryConstraint],
                                              partResp:Seq[Bucket], storeResp:Seq[Bucket]) {
    expect(dsMock.queryAggregateDigests(bucketing, constraints)).andReturn(participantResponse(partResp))
    store.queryDownstreams(EasyMock.eq(abPair), EasyMock.eq(constraints), anyUnitF5)
      expectLastCall[Unit].andAnswer(DownstreamVersionAnswer(storeResp))
  }

  protected def participantResponse(buckets:Seq[Bucket]) =
    buckets.map(b => AggregateDigest(AttributesUtil.toSeq(b.attrs), new DateTime, b.vsn))

  protected abstract class VersionAnswer[T] extends IAnswer[Unit] {
    def res:Seq[Bucket]

    def answer {
      val args = EasyMock.getCurrentArguments
      val cb = args(2).asInstanceOf[T]

      // We need to work through each bucket and its children.
      answerBuckets(res, cb)
    }

    def answerBuckets(buckets:Seq[Bucket], cb:T) {
      buckets.foreach(b => b.nextTx match {
        case entityTx:EntityTx => answerEntities(entityTx.entities, cb)
        case aggregateTx:AggregateTx => answerBuckets(aggregateTx.respBuckets, cb)
      })
    }

    def answerEntities(entities:Seq[Vsn], cb:T):Unit
  }

  protected case class UpstreamVersionAnswer(res:Seq[Bucket])
      extends VersionAnswer[Function4[VersionID, Map[String, String], DateTime, String, Unit]] {
    def answerEntities(entities:Seq[Vsn], cb:Function4[VersionID, Map[String, String], DateTime, String, Unit]) {
      entities.foreach { case Vsn(name, attributes, vsn) =>
        cb(VersionID(abPair, name), attributes.map { case (k, v) => k -> v.toString }.toMap, new DateTime, vsn)
      }
    }
  }
  protected case class DownstreamVersionAnswer(res:Seq[Bucket])
      extends VersionAnswer[Function5[VersionID, Map[String, String], DateTime, String, String, Unit]] {
    def answerEntities(entities:Seq[Vsn], cb:Function5[VersionID, Map[String, String], DateTime, String, String, Unit]) {
      entities.foreach { case Vsn(name, attributes, vsn) =>
        cb(VersionID(abPair, name), attributes.map { case (k, v) => k -> v.toString }.toMap, new DateTime, vsn, vsn)
      }
    }
  }
}
object AbstractDataDrivenPolicyTest {

  //
  // Scenarios
  //

  @DataPoint def datesOnlyScenario =
    AggregateTx(Map("bizDate" -> yearly), Seq(unbounded("bizDate")), Seq(
      Bucket("2010", Map("bizDate" -> "2010"), DigestUtils.md5Hex("vsn1" + "vsn2" + "vsn3"),
        AggregateTx(Map("bizDate" -> monthly), Seq(range("bizDate", START_2010, END_2010)), Seq(
          Bucket("2010-07", Map("bizDate" -> "2010-07"), DigestUtils.md5Hex("vsn1" + "vsn2"),
            AggregateTx(Map("bizDate" -> daily), Seq(range("bizDate", JUL_2010, END_JUL_2010)), Seq(
              Bucket("2010-07-08", Map("bizDate" -> "2010-07-08"), DigestUtils.md5Hex("vsn1"),
                EntityTx(Seq(range("bizDate", JUL_8_2010, END_JUL_8_2010)), Seq(
                  Vsn("id1", Map("bizDate" -> JUL_8_2010_1), "vsn1")
                ))),
              Bucket("2010-07-09", Map("bizDate" -> "2010-07-09"), DigestUtils.md5Hex("vsn2"),
                EntityTx(Seq(range("bizDate", JUL_9_2010, END_JUL_9_2010)), Seq(
                  Vsn("id2", Map("bizDate" -> JUL_8_2010_2), "vsn2")
                )))
            ))),
          Bucket("2010-08", Map("bizDate" -> "2010-08"), DigestUtils.md5Hex("vsn3"),
            AggregateTx(Map("bizDate" -> daily), Seq(range("bizDate", AUG_2010, END_AUG_2010)), Seq(
              Bucket("2010-08-02", Map("bizDate" -> "2010-08-02"), DigestUtils.md5Hex("vsn3"),
                EntityTx(Seq(range("bizDate", AUG_11_2010, END_AUG_11_2010)), Seq(
                  Vsn("id3", Map("bizDate" -> AUG_11_2010_1), "vsn3")
                )))
            )))
        ))),
      Bucket("2011", Map("bizDate" -> "2011"), DigestUtils.md5Hex("vsn4"),
        AggregateTx(Map("bizDate" -> monthly), Seq(range("bizDate", START_2011, END_2011)), Seq(
          Bucket("2011-01", Map("bizDate" -> "2011-01"), DigestUtils.md5Hex("vsn4"),
            AggregateTx(Map("bizDate" -> daily), Seq(range("bizDate", JAN_2011, END_JAN_2011)), Seq(
              Bucket("2011-01-20", Map("bizDate" -> "2011-01-20"), DigestUtils.md5Hex("vsn4"),
                EntityTx(Seq(range("bizDate", JAN_20_2011, END_JAN_20_2011)), Seq(
                  Vsn("id4", Map("bizDate" -> JAN_20_2011_1), "vsn4")
                )))
            )))
        )))
    ))

  
  //
  // Aliases
  //

  val yearly = YearlyCategoryFunction
  val monthly = MonthlyCategoryFunction
  val daily = DailyCategoryFunction
  val individual = IndividualCategoryFunction

  def unbounded(n:String) = UnboundedRangeQueryConstraint(n)
  def range(n:String, lower:AnyRef, upper:AnyRef) = RangeQueryConstraint(n, Seq(lower.toString, upper.toString))


  //
  // Type Definitions
  //

  class Tx
  case class AggregateTx(bucketing:Map[String, CategoryFunction], constraints:Seq[QueryConstraint], respBuckets:Seq[Bucket]) extends Tx
  case class EntityTx(constraints:Seq[QueryConstraint], entities:Seq[Vsn]) extends Tx

  case class Bucket(name:String, attrs:Map[String, String], vsn:String, nextTx:Tx)
  case class Vsn(id:String, attrs:Map[String, AnyRef], vsn:String)
}