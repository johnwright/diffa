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

import org.hibernate.cfg.Configuration
import org.junit.{Before, Test}
import org.joda.time.DateTime
import org.junit.Assert._
import net.lshift.diffa.kernel.util.DateUtils._
import net.lshift.diffa.kernel.util.Dates._
import net.lshift.diffa.kernel.events._
import collection.mutable.{ListBuffer, HashMap}
import net.lshift.diffa.kernel.participants._
import net.lshift.diffa.kernel.indexing.LuceneVersionCorrelationStore
import scala.collection.JavaConversions._
import net.lshift.diffa.kernel.participants.EasyConstraints._
import org.apache.lucene.store.{MMapDirectory, FSDirectory, RAMDirectory}
import java.io.File
import org.junit.runner.RunWith
import org.junit.experimental.theories.{Theory, DataPoint, Theories}

// TODO: [#162] Ensure ordering is preserved by id

/**
 * Test cases for the Hibernate backed VersionCorrelationStore.
 */
@RunWith(classOf[Theories])
class LuceneVersionCorrelationStoreTest {
  import LuceneVersionCorrelationStoreTest._

  private val pair = "pair"
  private val otherPair = "other-pair"
  private val emptyAttributes:Map[String, TypedAttribute] = Map()
  private val emptyStrAttributes:Map[String, String] = Map()

  protected val yearly = YearlyCategoryFunction
  protected val monthly = MonthlyCategoryFunction
  protected val daily = DailyCategoryFunction
  protected val individual = DailyCategoryFunction

  @Before
  def cleanupStore {
    flushStore
  }

  @Test
  def matchedPairs = {
    store.storeUpstreamVersion(VersionID(pair, "id1"), emptyAttributes, DEC_31_2009, "upstreamVsn")
    store.storeDownstreamVersion(VersionID(pair, "id1"), emptyAttributes, DEC_31_2009, "upstreamVsn", "downstreamVsn")

    val unmatched = store.unmatchedVersions(pair, Seq(NoConstraint("date")), Seq(NoConstraint("date")))
    assertEquals(0, unmatched.size)
  }

  @Test
  def constrainedMatchedPairsWithDifferentCategories = {
    store.storeUpstreamVersion(VersionID(pair, "id1"), dateAttributes, JUL_1_2010_1, "upstreamVsn")
    store.storeDownstreamVersion(VersionID(pair, "id1"), intAttributes, JUL_1_2010_1, "upstreamVsn", "downstreamVsn")

    val unmatched = store.unmatchedVersions(pair, dateConstraints, intConstraints)
    assertEquals(0, unmatched.size)
  }

  @Test
  def unmatchedPairFromUpstream = {
    val timestamp = new DateTime()
    store.storeUpstreamVersion(VersionID(pair, "id2"), emptyAttributes, DEC_31_2009, "upstreamVsn")

    val unmatched = store.unmatchedVersions(pair, Seq(NoConstraint("date")), Seq(NoConstraint("date")))
    assertEquals(1, unmatched.size)
    assertCorrelationEquals(Correlation(null, pair, "id2", emptyStrAttributes, emptyStrAttributes, DEC_31_2009, timestamp, "upstreamVsn", null, null, false), unmatched(0))
  }

  @Theory
  def constrainedAndIncludedUnmatchedPairFromUpstream(system:AttributeSystem) = {
    val timestamp = new DateTime()
    store.storeUpstreamVersion(VersionID(pair, "id2"), system.includedAttrs, DEC_31_2009, "upstreamVsn")

    val unmatched = store.unmatchedVersions(pair, system.constraints, noDateConstraint)
    assertEquals(1, unmatched.size)
    assertCorrelationEquals(Correlation(null, pair, "id2", system.includedStrAttrs, emptyStrAttributes, DEC_31_2009, timestamp, "upstreamVsn", null, null, false), unmatched(0))
  }

  @Theory
  def constrainedAndExcludedUnmatchedPairFromUpstream(system:AttributeSystem) = {
    val timestamp = new DateTime()
    store.storeUpstreamVersion(VersionID(pair, "id2"), system.excludedAttrs, DEC_31_2009, "upstreamVsn")

    val unmatched = store.unmatchedVersions(pair, system.constraints, noDateConstraint)
    assertEquals(0, unmatched.size)
  }

  @Test
  def unmatchPairFromUpstreamShouldBeIndicatedInReturnValue {
    val timestamp = new DateTime()
    val corr = store.storeUpstreamVersion(VersionID(pair, "id2"), emptyAttributes, DEC_31_2009, "upstreamVsn")
    assertCorrelationEquals(Correlation(null, pair, "id2", emptyStrAttributes, emptyStrAttributes, DEC_31_2009, timestamp, "upstreamVsn", null, null, false), corr)
  }

  @Test
  def unmatchedPairFromDownstream = {
    val timestamp = new DateTime()
    store.storeDownstreamVersion(VersionID(pair, "id3"), emptyAttributes, DEC_31_2009, "upstreamVsn", "downstreamVsn")

    val unmatched = store.unmatchedVersions(pair, Seq(NoConstraint("date")), Seq(NoConstraint("date")))
    assertEquals(1, unmatched.size)
    assertCorrelationEquals(Correlation(null, pair, "id3", emptyStrAttributes, emptyStrAttributes, DEC_31_2009, timestamp,  null, "upstreamVsn", "downstreamVsn", false), unmatched(0))
  }

  @Test
  def unmatchedPairFromDownstreamShouldBeIndicatedInReturnValue {
    val timestamp = new DateTime()
    val corr = store.storeDownstreamVersion(VersionID(pair, "id3"), emptyAttributes, DEC_31_2009, "upstreamVsn", "downstreamVsn")
    assertCorrelationEquals(Correlation(null, pair, "id3", emptyStrAttributes, emptyStrAttributes, DEC_31_2009, timestamp, null, "upstreamVsn", "downstreamVsn", false), corr)
  }

  @Test
  def matchedPairsAfterChanges = {
    store.storeUpstreamVersion(VersionID(pair, "id4"), emptyAttributes, DEC_31_2009, "upstreamVsnA")
    store.storeUpstreamVersion(VersionID(pair, "id4"), emptyAttributes, DEC_31_2009, "upstreamVsnB")
    store.storeDownstreamVersion(VersionID(pair, "id4"), emptyAttributes, DEC_31_2009, "upstreamVsnA", "downstreamVsnA")
    store.storeDownstreamVersion(VersionID(pair, "id4"), emptyAttributes, DEC_31_2009, "upstreamVsnB", "downstreamVsnB")

    val unmatched = store.unmatchedVersions(pair, Seq(NoConstraint("date")), Seq(NoConstraint("date")))
    assertEquals(0, unmatched.size)
  }

  @Test
  def unmatchedPairsAfterChanges = {
    val timestamp = new DateTime()
    store.storeUpstreamVersion(VersionID(pair, "id5"), emptyAttributes,DEC_31_2009, "upstreamVsnA")
    store.storeDownstreamVersion(VersionID(pair, "id5"), emptyAttributes, DEC_31_2009, "upstreamVsnA", "downstreamVsnA")
    store.storeUpstreamVersion(VersionID(pair, "id5"), emptyAttributes, DEC_31_2009, "upstreamVsnB")

    val unmatched = store.unmatchedVersions(pair, Seq(NoConstraint("date")), Seq(NoConstraint("date")))
    assertEquals(1, unmatched.size)
    assertCorrelationEquals(Correlation(null, pair, "id5", emptyStrAttributes, emptyStrAttributes, DEC_31_2009, timestamp, "upstreamVsnB", "upstreamVsnA", "downstreamVsnA", false), unmatched(0))
  }

  @Test
  def unmatchedPairsAfterChangesShouldBeIndicatedInReturnValue = {
    val timestamp = new DateTime()
    store.storeUpstreamVersion(VersionID(pair, "id5"), emptyAttributes, DEC_31_2009, "upstreamVsnA")
    store.storeDownstreamVersion(VersionID(pair, "id5"), emptyAttributes, DEC_31_2009, "upstreamVsnA", "downstreamVsnA")
    val corr = store.storeUpstreamVersion(VersionID(pair, "id5"), emptyAttributes, DEC_31_2009, "upstreamVsnB")

    assertCorrelationEquals(Correlation(null, pair, "id5", emptyStrAttributes, emptyStrAttributes, DEC_31_2009, timestamp, "upstreamVsnB", "upstreamVsnA", "downstreamVsnA", false), corr)
  }

  @Test
  def deletingSource = {
    store.storeUpstreamVersion(VersionID(pair, "id6"), bizDateMap(DEC_1_2009), DEC_1_2009, "upstreamVsn-id6")
    store.storeUpstreamVersion(VersionID(pair, "id7"), bizDateMap(DEC_1_2009), DEC_1_2009, "upstreamVsn-id7")

    val corr = store.clearUpstreamVersion(VersionID(pair, "id6"))
    assertCorrelationEquals(Correlation(null, pair, "id6", null, null, null, null, null, null, null, true), corr)

    val collector = new Collector
    store.queryUpstreams(pair, List(DateRangeConstraint("bizDate", DEC_1_2009, endOfDay(DEC_1_2009))), collector.collectUpstream)
    assertEquals(
      List(UpstreamPairChangeEvent(VersionID(pair, "id7"), bizDateSeq(DEC_1_2009), DEC_1_2009, "upstreamVsn-id7")),
      collector.upstreamObjs.toList)
  }

  @Test
  def deletingSourceThatIsMatched = {
    store.storeUpstreamVersion(VersionID(pair, "id6"), emptyAttributes, DEC_1_2009, "upstreamVsn-id6")
    store.storeDownstreamVersion(VersionID(pair, "id6"), emptyAttributes, DEC_1_2009, "upstreamVsn-id6", "downstreamVsn-id6")
    store.clearUpstreamVersion(VersionID(pair, "id6"))

    val collector = new Collector
    store.queryUpstreams(pair, List(DateRangeConstraint("bizDate", DEC_1_2009, endOfDay(DEC_1_2009))), collector.collectUpstream)
    assertEquals(0, collector.upstreamObjs.size)
  }

  @Test
  def deletingDest = {
    store.storeDownstreamVersion(VersionID(pair, "id6"), bizDateMap(DEC_1_2009), DEC_1_2009, "upstreamVsn-id6", "downstreamVsn-id6")
    store.storeDownstreamVersion(VersionID(pair, "id7"), bizDateMap(DEC_1_2009), DEC_1_2009, "upstreamVsn-id7", "downstreamVsn-id7")

    val corr = store.clearDownstreamVersion(VersionID(pair, "id6"))
    assertCorrelationEquals(Correlation(null, pair, "id6", null, null, null, null, null, null, null, true), corr)

    val collector = new Collector
    val digests = store.queryDownstreams(pair, List(DateRangeConstraint("bizDate", DEC_1_2009, endOfDay(DEC_1_2009))), collector.collectDownstream)
    assertEquals(
      List(DownstreamCorrelatedPairChangeEvent(VersionID(pair, "id7"), bizDateSeq(DEC_1_2009), DEC_1_2009, "upstreamVsn-id7", "downstreamVsn-id7")),
      collector.downstreamObjs.toList)
  }

  @Test
  def deletingDestThatIsMatched = {
    store.storeUpstreamVersion(VersionID(pair, "id6"), emptyAttributes, DEC_1_2009, "upstreamVsn-id6")
    store.storeDownstreamVersion(VersionID(pair, "id6"), emptyAttributes, DEC_1_2009, "upstreamVsn-id6", "downstreamVsn-id6")
    store.clearDownstreamVersion(VersionID(pair, "id6"))

    val collector = new Collector
    store.queryDownstreams(pair, List(DateRangeConstraint("bizDate", DEC_1_2009, endOfDay(DEC_1_2009))), collector.collectDownstream)
    assertEquals(0, collector.downstreamObjs.size)
  }

  // TODO: Theorise
  @Test
  def queryUpstreamRangeExcludesEarlier = {
    store.storeUpstreamVersion(VersionID(pair, "id6"), bizDateMap(DEC_1_2009), DEC_1_2009, "upstreamVsn-id6")
    store.storeUpstreamVersion(VersionID(pair, "id7"), bizDateMap(DEC_2_2009), DEC_2_2009, "upstreamVsn-id7")

    val collector = new Collector
    val digests = store.queryUpstreams(pair, List(DateRangeConstraint("bizDate", DEC_2_2009, endOfDay(DEC_2_2009))), collector.collectUpstream)
    assertEquals(
      List(UpstreamPairChangeEvent(VersionID(pair, "id7"), bizDateSeq(DEC_2_2009), DEC_2_2009, "upstreamVsn-id7")),
      collector.upstreamObjs.toList)
  }

  // TODO: Theorise
  @Test
  def queryUpstreamRangeExcludesLater = {
    store.storeUpstreamVersion(VersionID(pair, "id6"), bizDateMap(DEC_1_2009), DEC_1_2009, "upstreamVsn-id6")
    store.storeUpstreamVersion(VersionID(pair, "id7"), bizDateMap(DEC_2_2009), DEC_2_2009, "upstreamVsn-id7")

    val collector = new Collector
    val digests = store.queryUpstreams(pair, List(DateRangeConstraint("bizDate", DEC_1_2009, endOfDay(DEC_1_2009))), collector.collectUpstream)
    assertEquals(
      List(UpstreamPairChangeEvent(VersionID(pair, "id6"), bizDateSeq(DEC_1_2009), DEC_1_2009, "upstreamVsn-id6")),
      collector.upstreamObjs.toList)
  }

  @Test
  def queryUpstreamRangeReturnsInIDOrder = {
    store.storeUpstreamVersion(VersionID(pair, "id7"), bizDateMap(DEC_2_2009), DEC_2_2009, "upstreamVsn-id7")
    store.storeUpstreamVersion(VersionID(pair, "id6"), bizDateMap(DEC_1_2009), DEC_1_2009, "upstreamVsn-id6")

    val collector = new Collector
    val digests = store.queryUpstreams(pair, List(unconstrainedDate("bizDate")), collector.collectUpstream)
    assertEquals(
      List(
        UpstreamPairChangeEvent(VersionID(pair, "id6"), bizDateSeq(DEC_1_2009), DEC_1_2009, "upstreamVsn-id6"),
        UpstreamPairChangeEvent(VersionID(pair, "id7"), bizDateSeq(DEC_2_2009), DEC_2_2009, "upstreamVsn-id7")),
      collector.upstreamObjs.toList)
  }

  // TODO: Theorise
  @Test
  def queryDownstreamRangeExcludesEarlier = {
    store.storeDownstreamVersion(VersionID(pair, "id6"), bizDateMap(DEC_1_2009), DEC_1_2009, "upstreamVsn-id6", "downstreamVsn-id6")
    store.storeDownstreamVersion(VersionID(pair, "id7"), bizDateMap(DEC_2_2009), DEC_2_2009, "upstreamVsn-id7", "downstreamVsn-id7")

    val collector = new Collector
    val digests = store.queryDownstreams(pair, List(DateRangeConstraint("bizDate", DEC_2_2009, endOfDay(DEC_2_2009))), collector.collectDownstream)
    assertEquals(
      List(DownstreamCorrelatedPairChangeEvent(VersionID(pair, "id7"), bizDateSeq(DEC_2_2009), DEC_2_2009, "upstreamVsn-id7", "downstreamVsn-id7")),
      collector.downstreamObjs.toList)
  }

  // TODO: Theorise
  @Test
  def queryDownstreamRangeExcludesLater = {
    store.storeDownstreamVersion(VersionID(pair, "id6"), bizDateMap(DEC_1_2009), DEC_1_2009, "upstreamVsn-id6", "downstreamVsn-id6")
    store.storeDownstreamVersion(VersionID(pair, "id7"), bizDateMap(DEC_2_2009), DEC_2_2009, "upstreamVsn-id7", "downstreamVsn-id7")

    val collector = new Collector
    val digests = store.queryDownstreams(pair, List(DateRangeConstraint("bizDate", DEC_1_2009, endOfDay(DEC_1_2009))), collector.collectDownstream)
    assertEquals(
      List(DownstreamCorrelatedPairChangeEvent(VersionID(pair, "id6"), bizDateSeq(DEC_1_2009), DEC_1_2009, "upstreamVsn-id6", "downstreamVsn-id6")),
      collector.downstreamObjs.toList)
  }

  @Test
  def queryDownstreamRangeReturnsInIDOrder = {
    store.storeDownstreamVersion(VersionID(pair, "id7"), bizDateMap(DEC_2_2009), DEC_2_2009, "upstreamVsn-id7", "downstreamVsn-id7")
    store.storeDownstreamVersion(VersionID(pair, "id6"), bizDateMap(DEC_1_2009), DEC_1_2009, "upstreamVsn-id6", "downstreamVsn-id6")

    val collector = new Collector
    val digests = store.queryDownstreams(pair, List(unconstrainedDate("bizDate")), collector.collectDownstream)
    assertEquals(
      List(
        DownstreamCorrelatedPairChangeEvent(VersionID(pair, "id6"), bizDateSeq(DEC_1_2009), DEC_1_2009, "upstreamVsn-id6", "downstreamVsn-id6"),
        DownstreamCorrelatedPairChangeEvent(VersionID(pair, "id7"), bizDateSeq(DEC_2_2009), DEC_2_2009, "upstreamVsn-id7", "downstreamVsn-id7")),
      collector.downstreamObjs.toList)
  }
  
  @Test
  def storedUpstreamShouldBeRetrievable = {
    val timestamp = new DateTime()
    store.storeUpstreamVersion(VersionID(pair, "id23"), emptyAttributes, DEC_1_2009, "upstreamVsn-id23")
    val corr = store.retrieveCurrentCorrelation(VersionID(pair, "id23")).getOrElse(null)

    assertCorrelationEquals(
      Correlation(null, pair, "id23", emptyStrAttributes, null, DEC_1_2009, timestamp, "upstreamVsn-id23", null, null, false),
      corr)
  }

  @Test
  def storedDownstreamShouldBeRetrievable = {
    val timestamp = new DateTime()
    store.storeDownstreamVersion(VersionID(pair, "id23"), emptyAttributes, DEC_1_2009, "upstreamVsn-id23", "downstreamVsn-id23")
    val corr = store.retrieveCurrentCorrelation(VersionID(pair, "id23")).getOrElse(null)

    assertCorrelationEquals(
      Correlation(null, pair, "id23", null, emptyStrAttributes, DEC_1_2009, timestamp, null, "upstreamVsn-id23", "downstreamVsn-id23", false),
      corr)
  }

  @Test
  def storedMatchShouldBeRetrievable = {
    val timestamp = new DateTime()
    store.storeUpstreamVersion(VersionID(pair, "id23"), emptyAttributes, DEC_1_2009, "upstreamVsn-id23")
    store.storeDownstreamVersion(VersionID(pair, "id23"), emptyAttributes, DEC_1_2009, "upstreamVsn-id23", "downstreamVsn-id23")
    val corr = store.retrieveCurrentCorrelation(VersionID(pair, "id23")).getOrElse(null)

    assertCorrelationEquals(
      Correlation(null, pair, "id23", emptyStrAttributes, emptyStrAttributes, DEC_1_2009, timestamp, "upstreamVsn-id23", "upstreamVsn-id23", "downstreamVsn-id23", true),
      corr)
  }

  @Test
  def unknownCorrelationShouldNotBeRetrievable = {
    val corr = store.retrieveCurrentCorrelation(VersionID(pair, "id99-missing"))
    assertEquals(None, corr)
  }

  private def assertCorrelationEquals(expected:Correlation, actual:Correlation) {
    if (expected == null) {
      assertNull(actual)
    } else {
      assertNotNull(actual)

      assertEquals(expected.id, actual.id)
      assertEquals(expected.pairing, actual.pairing)
      assertEquals(expected.upstreamVsn, actual.upstreamVsn)
      assertEquals(expected.downstreamUVsn, actual.downstreamUVsn)
      assertEquals(expected.downstreamDVsn, actual.downstreamDVsn)
      assertEquals(expected.upstreamAttributes, actual.upstreamAttributes)
      assertEquals(expected.downstreamAttributes, actual.downstreamAttributes)
    }
  }
}

class Collector {
  val upstreamObjs = new ListBuffer[UpstreamPairChangeEvent]
  val downstreamObjs = new ListBuffer[DownstreamCorrelatedPairChangeEvent]

  def collectUpstream(id:VersionID, attributes:Map[String, String], lastUpdate:DateTime, vsn:String) = {
    upstreamObjs += UpstreamPairChangeEvent(id, AttributesUtil.toSeq(attributes), lastUpdate, vsn)
  }
  def collectDownstream(id:VersionID, attributes:Map[String, String], lastUpdate:DateTime, uvsn:String, dvsn:String) = {
    downstreamObjs += DownstreamCorrelatedPairChangeEvent(id, AttributesUtil.toSeq(attributes), lastUpdate, uvsn, dvsn)
  }
}

object LuceneVersionCorrelationStoreTest {
  val indexer = new LuceneVersionCorrelationStore(new MMapDirectory(new File("target")))
  val store:VersionCorrelationStore = indexer

  // Helper methods for various constraint/attribute scenarios
  def bizDateSeq(d:DateTime) = Seq(d.toString())
  def bizDateMap(d:DateTime) = Map("bizDate" -> DateAttribute(d))
  def intMap(i:Int) = Map("someInt" -> IntegerAttribute(i))

  // Standard attribute/constraint definitions
  private val dateAttributes = bizDateMap(JUL_1_2010_1)
  private val excludedDateAttributes = bizDateMap(AUG_11_2010_1)
  private val dateConstraints = Seq(DateRangeConstraint("bizDate", JUL_2010, END_JUL_2010))
  private val noDateConstraint = Seq(NoConstraint("bizDate"))
  private val intAttributes = intMap(2500)
  private val excludedIntAttributes = intMap(20000)
  private val intConstraints = Seq(IntegerRangeConstraint("someInt", 2000, 2999))

  // Defines a testable combination of constraints/attributes the store should be able to handle
  case class AttributeSystem(constraints:Seq[QueryConstraint], includedAttrs:Map[String, TypedAttribute], excludedAttrs:Map[String, TypedAttribute]) {
    def includedStrAttrs = includedAttrs.map { case (k, v) => k -> v.value }.toMap
    def excludedStrAttrs = excludedAttrs.map { case (k, v) => k -> v.value }.toMap
  }
  @DataPoint def dates = AttributeSystem(dateConstraints, dateAttributes, excludedDateAttributes)
  @DataPoint def ints = AttributeSystem(intConstraints, intAttributes, excludedIntAttributes)

  def flushStore = {
//    val s = sessionFactory.openSession
//    s.createCriteria(classOf[Correlation]).list.foreach(p => s.delete(p))
//    s.flush
//    s.close
    indexer.reset
  }
}