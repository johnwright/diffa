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
import net.lshift.diffa.kernel.indexing.{LuceneVersionCorrelationStore, LuceneVersionCorrelationStoreFactory}
import scala.collection.JavaConversions._
import net.lshift.diffa.kernel.participants.EasyConstraints._
import org.apache.lucene.store.{MMapDirectory, FSDirectory, RAMDirectory}
import java.io.File
import org.junit.runner.RunWith
import org.junit.experimental.theories.{DataPoints, Theory, DataPoint, Theories}
import net.lshift.diffa.kernel.config.ConfigStore
import org.easymock.EasyMock

/**
 * Test cases for the Hibernate backed VersionCorrelationStore.
 */
@RunWith(classOf[Theories])
class LuceneVersionCorrelationStoreTest {
  import LuceneVersionCorrelationStoreTest._

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
    val session = store.startSession()
    session.storeUpstreamVersion(VersionID(pair, "id1"), emptyAttributes, DEC_31_2009, "upstreamVsn")
    session.storeDownstreamVersion(VersionID(pair, "id1"), emptyAttributes, DEC_31_2009, "upstreamVsn", "downstreamVsn")
    session.flush()

    val unmatched = store.unmatchedVersions(Seq(NoConstraint("date")), Seq(NoConstraint("date")))
    assertEquals(0, unmatched.size)
  }

  @Test
  def constrainedMatchedPairsWithDifferentCategories = {
    val session = store.startSession()
    session.storeUpstreamVersion(VersionID(pair, "id1"), dateAttributes, JUL_1_2010_1, "upstreamVsn")
    session.storeDownstreamVersion(VersionID(pair, "id1"), intAttributes, JUL_1_2010_1, "upstreamVsn", "downstreamVsn")
    session.flush()

    val unmatched = store.unmatchedVersions(dateConstraints, intConstraints)
    assertEquals(0, unmatched.size)
  }

  @Test
  def unmatchedPairFromUpstream = {
    val session = store.startSession()
    val timestamp = new DateTime()
    session.storeUpstreamVersion(VersionID(pair, "id2"), emptyAttributes, DEC_31_2009, "upstreamVsn")
    session.flush()

    val unmatched = store.unmatchedVersions(Seq(NoConstraint("date")), Seq(NoConstraint("date")))
    assertEquals(1, unmatched.size)
    assertCorrelationEquals(Correlation(null, pair, "id2", emptyStrAttributes, emptyStrAttributes, DEC_31_2009, timestamp, "upstreamVsn", null, null, false), unmatched(0))
  }

  @Theory
  def constrainedAndIncludedUnmatchedPairFromUpstream(system:AttributeSystem) = {
    val timestamp = new DateTime()
    val session = store.startSession()
    session.storeUpstreamVersion(VersionID(pair, "id2"), system.includedAttrs, DEC_31_2009, "upstreamVsn")
    session.flush()

    val unmatched = store.unmatchedVersions(system.constraints, noDateConstraint)
    assertEquals(1, unmatched.size)
    assertCorrelationEquals(Correlation(null, pair, "id2", system.includedStrAttrs, emptyStrAttributes, DEC_31_2009, timestamp, "upstreamVsn", null, null, false), unmatched(0))
  }

  @Theory
  def constrainedAndExcludedUnmatchedPairFromUpstream(system:AttributeSystem) = {
    val timestamp = new DateTime()
    val session = store.startSession()
    session.storeUpstreamVersion(VersionID(pair, "id2"), system.excludedAttrs, DEC_31_2009, "upstreamVsn")
    session.flush()

    val unmatched = store.unmatchedVersions(system.constraints, noDateConstraint)
    assertEquals(0, unmatched.size)
  }

  @Test
  def unmatchedPairFromUpstreamShouldBeIndicatedInReturnValue {
    val timestamp = new DateTime()
    val session = store.startSession()
    val corr = session.storeUpstreamVersion(VersionID(pair, "id2"), emptyAttributes, DEC_31_2009, "upstreamVsn")
    session.flush()

    assertCorrelationEquals(Correlation(null, pair, "id2", emptyStrAttributes, emptyStrAttributes, DEC_31_2009, timestamp, "upstreamVsn", null, null, false), corr)
  }

  @Test
  def unmatchedPairFromDownstream = {
    val session = store.startSession()
    val timestamp = new DateTime()
    session.storeDownstreamVersion(VersionID(pair, "id3"), emptyAttributes, DEC_31_2009, "upstreamVsn", "downstreamVsn")
    session.flush()

    val unmatched = store.unmatchedVersions(Seq(NoConstraint("date")), Seq(NoConstraint("date")))
    assertEquals(1, unmatched.size)
    assertCorrelationEquals(Correlation(null, pair, "id3", emptyStrAttributes, emptyStrAttributes, DEC_31_2009, timestamp,  null, "upstreamVsn", "downstreamVsn", false), unmatched(0))
  }

  @Test
  def unmatchedPairFromDownstreamShouldBeIndicatedInReturnValue {
    val timestamp = new DateTime()
    val session = store.startSession()
    val corr = session.storeDownstreamVersion(VersionID(pair, "id3"), emptyAttributes, DEC_31_2009, "upstreamVsn", "downstreamVsn")
    session.flush()
    assertCorrelationEquals(Correlation(null, pair, "id3", emptyStrAttributes, emptyStrAttributes, DEC_31_2009, timestamp, null, "upstreamVsn", "downstreamVsn", false), corr)
  }

  @Test
  def matchedPairsAfterChanges = {
    val session = store.startSession()
    session.storeUpstreamVersion(VersionID(pair, "id4"), emptyAttributes, DEC_31_2009, "upstreamVsnA")
    session.storeUpstreamVersion(VersionID(pair, "id4"), emptyAttributes, DEC_31_2009, "upstreamVsnB")
    session.storeDownstreamVersion(VersionID(pair, "id4"), emptyAttributes, DEC_31_2009, "upstreamVsnA", "downstreamVsnA")
    session.storeDownstreamVersion(VersionID(pair, "id4"), emptyAttributes, DEC_31_2009, "upstreamVsnB", "downstreamVsnB")
    session.flush()

    val unmatched = store.unmatchedVersions(Seq(NoConstraint("date")), Seq(NoConstraint("date")))
    assertEquals(0, unmatched.size)
  }

  @Test
  def unmatchedPairsAfterChanges = {
    val timestamp = new DateTime()

    val session = store.startSession()
    session.storeUpstreamVersion(VersionID(pair, "id5"), emptyAttributes,DEC_31_2009, "upstreamVsnA")
    session.storeDownstreamVersion(VersionID(pair, "id5"), emptyAttributes, DEC_31_2009, "upstreamVsnA", "downstreamVsnA")
    session.storeUpstreamVersion(VersionID(pair, "id5"), emptyAttributes, DEC_31_2009, "upstreamVsnB")
    session.flush()

    val unmatched = store.unmatchedVersions(Seq(NoConstraint("date")), Seq(NoConstraint("date")))
    assertEquals(1, unmatched.size)
    assertCorrelationEquals(Correlation(null, pair, "id5", emptyStrAttributes, emptyStrAttributes, DEC_31_2009, timestamp, "upstreamVsnB", "upstreamVsnA", "downstreamVsnA", false), unmatched(0))
  }

  @Test
  def unmatchedPairsAfterChangesShouldBeIndicatedInReturnValue = {
    val timestamp = new DateTime()

    val session = store.startSession()
    session.storeUpstreamVersion(VersionID(pair, "id5"), emptyAttributes, DEC_31_2009, "upstreamVsnA")
    session.storeDownstreamVersion(VersionID(pair, "id5"), emptyAttributes, DEC_31_2009, "upstreamVsnA", "downstreamVsnA")
    val corr = session.storeUpstreamVersion(VersionID(pair, "id5"), emptyAttributes, DEC_31_2009, "upstreamVsnB")
    session.flush()

    assertCorrelationEquals(Correlation(null, pair, "id5", emptyStrAttributes, emptyStrAttributes, DEC_31_2009, timestamp, "upstreamVsnB", "upstreamVsnA", "downstreamVsnA", false), corr)
  }

  @Test
  def deletingSource = {
    val session = store.startSession()
    session.storeUpstreamVersion(VersionID(pair, "id6"), bizDateMap(DEC_1_2009), DEC_1_2009, "upstreamVsn-id6")
    session.storeUpstreamVersion(VersionID(pair, "id7"), bizDateMap(DEC_1_2009), DEC_1_2009, "upstreamVsn-id7")
    val corr = session.clearUpstreamVersion(VersionID(pair, "id6"))
    session.flush()
    assertCorrelationEquals(Correlation(null, pair, "id6", null, null, null, null, null, null, null, true), corr)

    val collector = new Collector
    store.queryUpstreams(List(DateRangeConstraint("bizDate", DEC_1_2009, endOfDay(DEC_1_2009))), collector.collectUpstream)
    assertEquals(
      List(UpstreamPairChangeEvent(VersionID(pair, "id7"), bizDateSeq(DEC_1_2009), DEC_1_2009, "upstreamVsn-id7")),
      collector.upstreamObjs.toList)
  }

  @Theory
  def deletingSourceThatIsMatched(system:AttributeSystem) = {
    val session = store.startSession()
    session.storeUpstreamVersion(VersionID(pair, "id6"), system.includedAttrs, DEC_1_2009, "upstreamVsn-id6")
    session.storeDownstreamVersion(VersionID(pair, "id6"), system.includedAttrs, DEC_1_2009, "upstreamVsn-id6", "downstreamVsn-id6")
    session.clearUpstreamVersion(VersionID(pair, "id6"))
    session.flush()

    val collector = new Collector
    store.queryUpstreams(List(DateRangeConstraint("bizDate", DEC_1_2009, endOfDay(DEC_1_2009))), collector.collectUpstream)
    assertEquals(0, collector.upstreamObjs.size)
  }

  @Test
  def deletingDest = {
    val session1 = store.startSession()
    session1.storeDownstreamVersion(VersionID(pair, "id6"), bizDateMap(DEC_1_2009), DEC_1_2009, "upstreamVsn-id6", "downstreamVsn-id6")
    session1.storeDownstreamVersion(VersionID(pair, "id7"), bizDateMap(DEC_1_2009), DEC_1_2009, "upstreamVsn-id7", "downstreamVsn-id7")
    session1.flush()

    val session2 = store.startSession()
    val corr = session2.clearDownstreamVersion(VersionID(pair, "id6"))
    session2.flush()
    assertCorrelationEquals(Correlation(null, pair, "id6", null, null, null, null, null, null, null, true), corr)

    val collector = new Collector
    val digests = store.queryDownstreams(List(DateRangeConstraint("bizDate", DEC_1_2009, endOfDay(DEC_1_2009))), collector.collectDownstream)
    assertEquals(
      List(DownstreamCorrelatedPairChangeEvent(VersionID(pair, "id7"), bizDateSeq(DEC_1_2009), DEC_1_2009, "upstreamVsn-id7", "downstreamVsn-id7")),
      collector.downstreamObjs.toList)
  }

  @Theory
  def deletingDestThatIsMatched(system:AttributeSystem) = {
    val session = store.startSession()
    session.storeUpstreamVersion(VersionID(pair, "id6"), system.includedAttrs, DEC_1_2009, "upstreamVsn-id6")
    session.storeDownstreamVersion(VersionID(pair, "id6"), system.includedAttrs, DEC_1_2009, "upstreamVsn-id6", "downstreamVsn-id6")
    session.clearDownstreamVersion(VersionID(pair, "id6"))
    session.flush()

    val collector = new Collector
    store.queryDownstreams(List(DateRangeConstraint("bizDate", DEC_1_2009, endOfDay(DEC_1_2009))), collector.collectDownstream)
    assertEquals(0, collector.downstreamObjs.size)
  }

  @Theory
  def queryUpstreamRangeExcludesExcluded(system:AttributeSystem) = {
    val session = store.startSession()
    session.storeUpstreamVersion(VersionID(pair, "id1"), system.includedAttrs, DEC_31_2009, "upstreamVsn-id1")
    session.storeUpstreamVersion(VersionID(pair, "id2"), system.excludedAttrs, DEC_31_2009, "upstreamVsn-id2")
    session.flush()

    val collector = new Collector
    val digests = store.queryUpstreams(system.constraints, collector.collectUpstream)
    assertEquals(
      List(UpstreamPairChangeEvent(VersionID(pair, "id1"), AttributesUtil.toSeqFromTyped(system.includedAttrs), DEC_31_2009, "upstreamVsn-id1")),
      collector.upstreamObjs.toList)
  }

  @Theory
  def queryDownstreamRangeExcludesExcluded(system:AttributeSystem) = {
    val session = store.startSession()
    session.storeDownstreamVersion(VersionID(pair, "id1"), system.includedAttrs, DEC_31_2009, "upstreamVsn-id1", "downstreamVsn-id1")
    session.storeDownstreamVersion(VersionID(pair, "id2"), system.excludedAttrs, DEC_31_2009, "upstreamVsn-id2", "downstreamVsn-id1")
    session.flush()

    val collector = new Collector
    val digests = store.queryDownstreams(system.constraints, collector.collectDownstream)
    assertEquals(
      List(DownstreamCorrelatedPairChangeEvent(VersionID(pair, "id1"), AttributesUtil.toSeqFromTyped(system.includedAttrs), DEC_31_2009, "upstreamVsn-id1", "downstreamVsn-id1")),
      collector.downstreamObjs.toList)
  }

  @Test
  def queryUpstreamRangeReturnsInIDOrder = {
    val session = store.startSession()
    session.storeUpstreamVersion(VersionID(pair, "id7"), bizDateMap(DEC_2_2009), DEC_2_2009, "upstreamVsn-id7")
    session.storeUpstreamVersion(VersionID(pair, "id6"), bizDateMap(DEC_1_2009), DEC_1_2009, "upstreamVsn-id6")
    session.flush()

    val collector = new Collector
    val digests = store.queryUpstreams(List(unconstrainedDate("bizDate")), collector.collectUpstream)
    assertEquals(
      List(
        UpstreamPairChangeEvent(VersionID(pair, "id6"), bizDateSeq(DEC_1_2009), DEC_1_2009, "upstreamVsn-id6"),
        UpstreamPairChangeEvent(VersionID(pair, "id7"), bizDateSeq(DEC_2_2009), DEC_2_2009, "upstreamVsn-id7")),
      collector.upstreamObjs.toList)
  }

  @Test
  def queryDownstreamRangeReturnsInIDOrder = {
    val session = store.startSession()
    session.storeDownstreamVersion(VersionID(pair, "id7"), bizDateMap(DEC_2_2009), DEC_2_2009, "upstreamVsn-id7", "downstreamVsn-id7")
    session.storeDownstreamVersion(VersionID(pair, "id6"), bizDateMap(DEC_1_2009), DEC_1_2009, "upstreamVsn-id6", "downstreamVsn-id6")
    session.flush()

    val collector = new Collector
    val digests = store.queryDownstreams(List(unconstrainedDate("bizDate")), collector.collectDownstream)
    assertEquals(
      List(
        DownstreamCorrelatedPairChangeEvent(VersionID(pair, "id6"), bizDateSeq(DEC_1_2009), DEC_1_2009, "upstreamVsn-id6", "downstreamVsn-id6"),
        DownstreamCorrelatedPairChangeEvent(VersionID(pair, "id7"), bizDateSeq(DEC_2_2009), DEC_2_2009, "upstreamVsn-id7", "downstreamVsn-id7")),
      collector.downstreamObjs.toList)
  }
  
  @Test
  def storedUpstreamShouldBeRetrievable = {
    val timestamp = new DateTime()
    val session = store.startSession()
    session.storeUpstreamVersion(VersionID(pair, "id23"), emptyAttributes, DEC_1_2009, "upstreamVsn-id23")
    session.flush()
    val corr = store.retrieveCurrentCorrelation(VersionID(pair, "id23")).getOrElse(null)

    assertCorrelationEquals(
      Correlation(null, pair, "id23", emptyStrAttributes, null, DEC_1_2009, timestamp, "upstreamVsn-id23", null, null, false),
      corr)
  }

  @Test
  def storedDownstreamShouldBeRetrievable = {
    val timestamp = new DateTime()
    val session = store.startSession()
    session.storeDownstreamVersion(VersionID(pair, "id23"), emptyAttributes, DEC_1_2009, "upstreamVsn-id23", "downstreamVsn-id23")
    session.flush()
    val corr = store.retrieveCurrentCorrelation(VersionID(pair, "id23")).getOrElse(null)

    assertCorrelationEquals(
      Correlation(null, pair, "id23", null, emptyStrAttributes, DEC_1_2009, timestamp, null, "upstreamVsn-id23", "downstreamVsn-id23", false),
      corr)
  }

  @Test
  def storedMatchShouldBeRetrievable = {
    val timestamp = new DateTime()
    val session = store.startSession()
    session.storeUpstreamVersion(VersionID(pair, "id23"), emptyAttributes, DEC_1_2009, "upstreamVsn-id23")
    session.storeDownstreamVersion(VersionID(pair, "id23"), emptyAttributes, DEC_1_2009, "upstreamVsn-id23", "downstreamVsn-id23")
    session.flush()
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

  @Test
  def storesMustBeIsolatedByPairKey = {
    val session = store.startSession()
    val otherStore = stores(otherPair)
    val otherSession = otherStore.startSession()

    otherSession.storeUpstreamVersion(VersionID(otherPair, "123456789"), emptyAttributes, DEC_1_2009, "up-123456789")
    otherSession.storeDownstreamVersion(VersionID(otherPair, "123456789"), emptyAttributes, DEC_1_2009, "up-123456789", "down-123456789")
    otherSession.flush()

    assertCorrelationEquals(
      Correlation(null, otherPair, "123456789", Map(), Map(), DEC_1_2009, null, "up-123456789", "up-123456789", "down-123456789", true),
      otherStore.retrieveCurrentCorrelation(VersionID(otherPair, "123456789")).getOrElse(null))

    session.storeUpstreamVersion(VersionID(pair, "123456789"), emptyAttributes, DEC_1_2009, "up-987654321")
    session.flush()
    assertCorrelationEquals(
      Correlation(null, pair, "123456789", Map(), Map(), DEC_1_2009, null, "up-987654321", null, null, false),
      store.retrieveCurrentCorrelation(VersionID(pair, "123456789")).getOrElse(null))

    // re-check other store
    assertCorrelationEquals(
      Correlation(null, otherPair, "123456789", Map(), Map(), DEC_1_2009, null, "up-123456789", "up-123456789", "down-123456789", true),
      otherStore.retrieveCurrentCorrelation(VersionID(otherPair, "123456789")).getOrElse(null))
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
      assertEquals(expected.isMatched, actual.isMatched)
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
  val dummyConfigStore = EasyMock.createMock(classOf[ConfigStore])
  EasyMock.expect(dummyConfigStore.maybeConfigOption("correlationStore.schemaVersion")).andStubReturn(Some("0"))
  EasyMock.replay(dummyConfigStore)

  val pair = "pair"
  val stores = new LuceneVersionCorrelationStoreFactory("target", classOf[MMapDirectory], dummyConfigStore)
  val indexer = stores(pair)
  val store:VersionCorrelationStore = indexer

  // Helper methods for various constraint/attribute scenarios
  def bizDateSeq(d:DateTime) = Seq(d.toString())
  def bizDateMap(d:DateTime) = Map("bizDate" -> DateAttribute(d))
  def intMap(i:Int) = Map("someInt" -> IntegerAttribute(i))
  def stringMap(s:String) = Map("someString" -> StringAttribute(s))

  // Standard attribute/constraint definitions
  private val dateAttributes = bizDateMap(JUL_1_2010_1)
  private val excludedByEarlierDateAttributes = bizDateMap(FEB_15_2010)
  private val excludedByLaterDateAttributes = bizDateMap(AUG_11_2010_1)
  private val dateConstraints = Seq(DateRangeConstraint("bizDate", JUL_2010, END_JUL_2010))
  private val noDateConstraint = Seq(NoConstraint("bizDate"))

  private val intAttributes = intMap(2500)
  private val excludedIntAttributes = intMap(20000)
  private val intConstraints = Seq(IntegerRangeConstraint("someInt", 2000, 2999))
  private val stringAttributes = stringMap("abc")
  private val excludedStringAttributes = stringMap("def")
  private val stringConstraints = Seq(PrefixQueryConstraint("someString", "ab"))
  private val setConstraints = Seq(SetQueryConstraint("someString", Set("abc","abc123","abcdef")))

  // Defines a testable combination of constraints/attributes the store should be able to handle
  case class AttributeSystem(constraints:Seq[QueryConstraint], includedAttrs:Map[String, TypedAttribute], excludedAttrs:Map[String, TypedAttribute]) {
    def includedStrAttrs = includedAttrs.map { case (k, v) => k -> v.value }.toMap
    def excludedStrAttrs = excludedAttrs.map { case (k, v) => k -> v.value }.toMap
  }

  @DataPoints def dates = Array(
    AttributeSystem(dateConstraints, dateAttributes, excludedByLaterDateAttributes),
    AttributeSystem(dateConstraints, dateAttributes, excludedByEarlierDateAttributes)
  )
  @DataPoint def ints = AttributeSystem(intConstraints, intAttributes, excludedIntAttributes)
  @DataPoint def strings = AttributeSystem(stringConstraints, stringAttributes, excludedStringAttributes)
  @DataPoint def set = AttributeSystem(setConstraints, stringAttributes, excludedStringAttributes)
  @DataPoints def setAndDates = Array(
    AttributeSystem(dateConstraints ++ setConstraints, dateAttributes ++ stringAttributes, excludedByLaterDateAttributes ++ excludedStringAttributes),
    AttributeSystem(dateConstraints ++ setConstraints, dateAttributes ++ stringAttributes, dateAttributes ++ excludedStringAttributes),
    AttributeSystem(dateConstraints ++ setConstraints, dateAttributes ++ stringAttributes, excludedByLaterDateAttributes ++ stringAttributes)
  )

  def flushStore = {
    indexer.reset
  }
}