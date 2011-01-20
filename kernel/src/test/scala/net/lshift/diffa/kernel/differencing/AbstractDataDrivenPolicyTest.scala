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

  protected def setupStubs(scenario:Scenario) {
    expect(configStore.getPair(scenario.pair.key)).andReturn(scenario.pair).anyTimes
  }

  protected def expectUpstreamAggregateSync(pair:Pair, bucketing:Map[String, CategoryFunction], constraints:Seq[QueryConstraint],
                                            partResp:Seq[Bucket], storeResp:Seq[Bucket]) {
    expect(usMock.queryAggregateDigests(bucketing, constraints)).andReturn(participantResponse(partResp))
    store.queryUpstreams(EasyMock.eq(pair.key), EasyMock.eq(constraints), anyUnitF4)
      expectLastCall[Unit].andAnswer(UpstreamVersionAnswer(pair, storeResp))
  }
  protected def expectDownstreamAggregateSync(pair:Pair, bucketing:Map[String, CategoryFunction], constraints:Seq[QueryConstraint],
                                              partResp:Seq[Bucket], storeResp:Seq[Bucket]) {
    expect(dsMock.queryAggregateDigests(bucketing, constraints)).andReturn(participantResponse(partResp))
    store.queryDownstreams(EasyMock.eq(pair.key), EasyMock.eq(constraints), anyUnitF5)
      expectLastCall[Unit].andAnswer(DownstreamVersionAnswer(pair, storeResp))
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

  protected case class UpstreamVersionAnswer(pair:Pair, res:Seq[Bucket])
      extends VersionAnswer[Function4[VersionID, Map[String, String], DateTime, String, Unit]] {
    def answerEntities(entities:Seq[Vsn], cb:Function4[VersionID, Map[String, String], DateTime, String, Unit]) {
      entities.foreach { case Vsn(name, attributes, vsn) =>
        cb(VersionID(pair.key, name), attributes.map { case (k, v) => k -> v.toString }.toMap, new DateTime, vsn)
      }
    }
  }
  protected case class DownstreamVersionAnswer(pair:Pair, res:Seq[Bucket])
      extends VersionAnswer[Function5[VersionID, Map[String, String], DateTime, String, String, Unit]] {
    def answerEntities(entities:Seq[Vsn], cb:Function5[VersionID, Map[String, String], DateTime, String, String, Unit]) {
      entities.foreach { case Vsn(name, attributes, vsn) =>
        cb(VersionID(pair.key, name), attributes.map { case (k, v) => k -> v.toString }.toMap, new DateTime, vsn, vsn)
      }
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
    )))

  @DataPoint def integersOnlyScenario = Scenario(
    Pair(key = "bc", categories = Map("someInt" -> "int")),
    AggregateTx(Map("someInt" -> thousands), Seq(unbounded("someInt")), Seq(
      Bucket("1000", Map("someInt" -> "1000"), DigestUtils.md5Hex("vsn1" + "vsn2" + "vsn3"),
        AggregateTx(Map("someInt" -> hundreds), Seq(range("someInt", 1000, 1999)), Seq(
          Bucket("1200", Map("someInt" -> "1200"), DigestUtils.md5Hex("vsn1" + "vsn2"),
            AggregateTx(Map("someInt" -> tens), Seq(range("someInt", 1200, 1299)), Seq(
              Bucket("1230", Map("someInt" -> "1230"), DigestUtils.md5Hex("vsn1"),
                EntityTx(Seq(range("someInt", 1230, 1239)), Seq(
                  Vsn("id1", Map("someInt" -> 1234), "vsn1")
                ))),
              Bucket("1240", Map("someInt" -> "1240"), DigestUtils.md5Hex("vsn2"),
                EntityTx(Seq(range("someInt", 1240, 1249)), Seq(
                  Vsn("id2", Map("someInt" -> 1245), "vsn2")
                )))
            ))),
          Bucket("1300", Map("someInt" -> "1300"), DigestUtils.md5Hex("vsn3"),
            AggregateTx(Map("someInt" -> tens), Seq(range("someInt", 1300, 1399)), Seq(
              Bucket("1350", Map("someInt" -> "1350"), DigestUtils.md5Hex("vsn3"),
                EntityTx(Seq(range("someInt", 1350, 1359)), Seq(
                  Vsn("id3", Map("someInt" -> 1357), "vsn3")
                )))
            )))
        ))),
      Bucket("2000", Map("someInt" -> "2000"), DigestUtils.md5Hex("vsn4"),
        AggregateTx(Map("someInt" -> hundreds), Seq(range("someInt", 2000, 2999)), Seq(
          Bucket("2300", Map("someInt" -> "2300"), DigestUtils.md5Hex("vsn4"),
            AggregateTx(Map("someInt" -> tens), Seq(range("someInt", 2300, 2399)), Seq(
              Bucket("2340", Map("someInt" -> "2340"), DigestUtils.md5Hex("vsn4"),
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

  class Tx
  case class AggregateTx(bucketing:Map[String, CategoryFunction], constraints:Seq[QueryConstraint], respBuckets:Seq[Bucket]) extends Tx
  case class EntityTx(constraints:Seq[QueryConstraint], entities:Seq[Vsn]) extends Tx

  case class Bucket(name:String, attrs:Map[String, String], vsn:String, nextTx:Tx)
  case class Vsn(id:String, attrs:Map[String, Any], vsn:String)
}