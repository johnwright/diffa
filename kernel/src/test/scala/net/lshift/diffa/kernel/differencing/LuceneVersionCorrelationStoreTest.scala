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
import org.junit.Assert._
import net.lshift.diffa.kernel.util.DateUtils._
import net.lshift.diffa.kernel.util.FullDateTimes._
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
import org.joda.time.{LocalDate, DateTime}

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
    val writer = store.openWriter()
    writer.storeUpstreamVersion(VersionID(pair, "id1"), emptyAttributes, DEC_31_2009, "upstreamVsn")
    writer.storeDownstreamVersion(VersionID(pair, "id1"), emptyAttributes, DEC_31_2009, "upstreamVsn", "downstreamVsn")
    writer.flush()

    val unmatched = store.unmatchedVersions(Seq(NoConstraint("datetime")), Seq(NoConstraint("datetime")))
    assertEquals(0, unmatched.size)
  }

  @Test
  def constrainedMatchedPairsWithDifferentCategories = {
    val writer = store.openWriter()
    writer.storeUpstreamVersion(VersionID(pair, "id1"), dateTimeAttributes, JUL_1_2010_1, "upstreamVsn")
    writer.storeDownstreamVersion(VersionID(pair, "id1"), intAttributes, JUL_1_2010_1, "upstreamVsn", "downstreamVsn")
    writer.flush()

    val unmatched = store.unmatchedVersions(dateTimeConstraints, intConstraints)
    assertEquals(0, unmatched.size)
  }

  @Test
  def unmatchedPairFromUpstream = {
    val writer = store.openWriter()
    val timestamp = new DateTime()
    writer.storeUpstreamVersion(VersionID(pair, "id2"), emptyAttributes, DEC_31_2009, "upstreamVsn")
    writer.flush()

    val unmatched = store.unmatchedVersions(Seq(NoConstraint("datetime")), Seq(NoConstraint("datetime")))
    assertEquals(1, unmatched.size)
    assertCorrelationEquals(Correlation(null, pair, "id2", emptyStrAttributes, emptyStrAttributes, DEC_31_2009, timestamp, "upstreamVsn", null, null, false), unmatched(0))
  }

  @Theory
  def constrainedAndIncludedUnmatchedPairFromUpstream(system:AttributeSystem) = {
    val timestamp = new DateTime()
    val writer = store.openWriter()
    writer.storeUpstreamVersion(VersionID(pair, "id2"), system.includedAttrs, DEC_31_2009, "upstreamVsn")
    writer.flush()

    val unmatched = store.unmatchedVersions(system.constraints, noDateTimeConstraint)
    assertEquals(1, unmatched.size)
    assertCorrelationEquals(Correlation(null, pair, "id2", system.includedStrAttrs, emptyStrAttributes, DEC_31_2009, timestamp, "upstreamVsn", null, null, false), unmatched(0))
  }

  @Theory
  def constrainedAndExcludedUnmatchedPairFromUpstream(system:AttributeSystem) = {
    val timestamp = new DateTime()
    val writer = store.openWriter()
    writer.storeUpstreamVersion(VersionID(pair, "id2"), system.excludedAttrs, DEC_31_2009, "upstreamVsn")
    writer.flush()

    val unmatched = store.unmatchedVersions(system.constraints, noDateTimeConstraint)
    assertEquals(0, unmatched.size)
  }

  @Test
  def unmatchedPairFromUpstreamShouldBeIndicatedInReturnValue {
    val timestamp = new DateTime()
    val writer = store.openWriter()
    val corr = writer.storeUpstreamVersion(VersionID(pair, "id2"), emptyAttributes, DEC_31_2009, "upstreamVsn")
    writer.flush()

    assertCorrelationEquals(Correlation(null, pair, "id2", emptyStrAttributes, emptyStrAttributes, DEC_31_2009, timestamp, "upstreamVsn", null, null, false), corr)
  }

  @Test
  def unmatchedPairFromDownstream = {
    val writer = store.openWriter()
    val timestamp = new DateTime()
    writer.storeDownstreamVersion(VersionID(pair, "id3"), emptyAttributes, DEC_31_2009, "upstreamVsn", "downstreamVsn")
    writer.flush()

    val unmatched = store.unmatchedVersions(Seq(NoConstraint("datetime")), Seq(NoConstraint("datetime")))
    assertEquals(1, unmatched.size)
    assertCorrelationEquals(Correlation(null, pair, "id3", emptyStrAttributes, emptyStrAttributes, DEC_31_2009, timestamp,  null, "upstreamVsn", "downstreamVsn", false), unmatched(0))
  }

  @Test
  def unmatchedPairFromDownstreamShouldBeIndicatedInReturnValue {
    val timestamp = new DateTime()
    val writer = store.openWriter()
    val corr = writer.storeDownstreamVersion(VersionID(pair, "id3"), emptyAttributes, DEC_31_2009, "upstreamVsn", "downstreamVsn")
    writer.flush()
    assertCorrelationEquals(Correlation(null, pair, "id3", emptyStrAttributes, emptyStrAttributes, DEC_31_2009, timestamp, null, "upstreamVsn", "downstreamVsn", false), corr)
  }

  @Test
  def matchedPairsAfterChanges = {
    val writer = store.openWriter()
    writer.storeUpstreamVersion(VersionID(pair, "id4"), emptyAttributes, DEC_31_2009, "upstreamVsnA")
    writer.storeUpstreamVersion(VersionID(pair, "id4"), emptyAttributes, DEC_31_2009, "upstreamVsnB")
    writer.storeDownstreamVersion(VersionID(pair, "id4"), emptyAttributes, DEC_31_2009, "upstreamVsnA", "downstreamVsnA")
    writer.storeDownstreamVersion(VersionID(pair, "id4"), emptyAttributes, DEC_31_2009, "upstreamVsnB", "downstreamVsnB")
    writer.flush()

    val unmatched = store.unmatchedVersions(Seq(NoConstraint("datetime")), Seq(NoConstraint("datetime")))
    assertEquals(0, unmatched.size)
  }

  @Test
  def unmatchedPairsAfterChanges = {
    val timestamp = new DateTime()

    val writer = store.openWriter()
    writer.storeUpstreamVersion(VersionID(pair, "id5"), emptyAttributes,DEC_31_2009, "upstreamVsnA")
    writer.storeDownstreamVersion(VersionID(pair, "id5"), emptyAttributes, DEC_31_2009, "upstreamVsnA", "downstreamVsnA")
    writer.storeUpstreamVersion(VersionID(pair, "id5"), emptyAttributes, DEC_31_2009, "upstreamVsnB")
    writer.flush()

    val unmatched = store.unmatchedVersions(Seq(NoConstraint("datetime")), Seq(NoConstraint("datetime")))
    assertEquals(1, unmatched.size)
    assertCorrelationEquals(Correlation(null, pair, "id5", emptyStrAttributes, emptyStrAttributes, DEC_31_2009, timestamp, "upstreamVsnB", "upstreamVsnA", "downstreamVsnA", false), unmatched(0))
  }

  @Test
  def unmatchedPairsAfterChangesShouldBeIndicatedInReturnValue = {
    val timestamp = new DateTime()

    val writer = store.openWriter()
    writer.storeUpstreamVersion(VersionID(pair, "id5"), emptyAttributes, DEC_31_2009, "upstreamVsnA")
    writer.storeDownstreamVersion(VersionID(pair, "id5"), emptyAttributes, DEC_31_2009, "upstreamVsnA", "downstreamVsnA")
    val corr = writer.storeUpstreamVersion(VersionID(pair, "id5"), emptyAttributes, DEC_31_2009, "upstreamVsnB")
    writer.flush()

    assertCorrelationEquals(Correlation(null, pair, "id5", emptyStrAttributes, emptyStrAttributes, DEC_31_2009, timestamp, "upstreamVsnB", "upstreamVsnA", "downstreamVsnA", false), corr)
  }

  @Test
  def deletingSource = {
    val writer = store.openWriter()
    writer.storeUpstreamVersion(VersionID(pair, "id6"), bizDateTimeMap(DEC_1_2009), DEC_1_2009, "upstreamVsn-id6")
    writer.storeUpstreamVersion(VersionID(pair, "id7"), bizDateTimeMap(DEC_1_2009), DEC_1_2009, "upstreamVsn-id7")
    val corr = writer.clearUpstreamVersion(VersionID(pair, "id6"))
    writer.flush()
    assertCorrelationEquals(Correlation(null, pair, "id6", null, null, null, null, null, null, null, true), corr)

    val collector = new Collector
    store.queryUpstreams(List(DateTimeRangeConstraint("bizDateTime", DEC_1_2009, endOfDay(DEC_1_2009))), collector.collectUpstream)
    assertEquals(
      List(UpstreamPairChangeEvent(VersionID(pair, "id7"), bizDateTimeSeq(DEC_1_2009), DEC_1_2009, "upstreamVsn-id7")),
      collector.upstreamObjs.toList)
  }

  @Theory
  def deletingSourceThatIsMatched(system:AttributeSystem) = {
    val writer = store.openWriter()
    writer.storeUpstreamVersion(VersionID(pair, "id6"), system.includedAttrs, DEC_1_2009, "upstreamVsn-id6")
    writer.storeDownstreamVersion(VersionID(pair, "id6"), system.includedAttrs, DEC_1_2009, "upstreamVsn-id6", "downstreamVsn-id6")
    writer.clearUpstreamVersion(VersionID(pair, "id6"))
    writer.flush()

    val collector = new Collector
    store.queryUpstreams(List(DateTimeRangeConstraint("bizDateTime", DEC_1_2009, endOfDay(DEC_1_2009))), collector.collectUpstream)
    assertEquals(0, collector.upstreamObjs.size)
  }

  @Test
  def deletingDest = {
    val writer1 = store.openWriter()
    writer1.storeDownstreamVersion(VersionID(pair, "id6"), bizDateTimeMap(DEC_1_2009), DEC_1_2009, "upstreamVsn-id6", "downstreamVsn-id6")
    writer1.storeDownstreamVersion(VersionID(pair, "id7"), bizDateTimeMap(DEC_1_2009), DEC_1_2009, "upstreamVsn-id7", "downstreamVsn-id7")
    writer1.flush()

    val writer2 = store.openWriter()
    val corr = writer2.clearDownstreamVersion(VersionID(pair, "id6"))
    writer2.flush()
    assertCorrelationEquals(Correlation(null, pair, "id6", null, null, null, null, null, null, null, true), corr)

    val collector = new Collector
    val digests = store.queryDownstreams(List(DateTimeRangeConstraint("bizDateTime", DEC_1_2009, endOfDay(DEC_1_2009))), collector.collectDownstream)
    assertEquals(
      List(DownstreamCorrelatedPairChangeEvent(VersionID(pair, "id7"), bizDateTimeSeq(DEC_1_2009), DEC_1_2009, "upstreamVsn-id7", "downstreamVsn-id7")),
      collector.downstreamObjs.toList)
  }

  @Theory
  def deletingDestThatIsMatched(system:AttributeSystem) = {
    val writer = store.openWriter()
    writer.storeUpstreamVersion(VersionID(pair, "id6"), system.includedAttrs, DEC_1_2009, "upstreamVsn-id6")
    writer.storeDownstreamVersion(VersionID(pair, "id6"), system.includedAttrs, DEC_1_2009, "upstreamVsn-id6", "downstreamVsn-id6")
    writer.clearDownstreamVersion(VersionID(pair, "id6"))
    writer.flush()

    val collector = new Collector
    store.queryDownstreams(List(DateTimeRangeConstraint("bizDate", DEC_1_2009, endOfDay(DEC_1_2009))), collector.collectDownstream)
    assertEquals(0, collector.downstreamObjs.size)
  }

  @Theory
  def queryUpstreamRangeExcludesExcluded(system:AttributeSystem) = {
    val writer = store.openWriter()
    writer.storeUpstreamVersion(VersionID(pair, "id1"), system.includedAttrs, DEC_31_2009, "upstreamVsn-id1")
    writer.storeUpstreamVersion(VersionID(pair, "id2"), system.excludedAttrs, DEC_31_2009, "upstreamVsn-id2")
    writer.flush()

    val collector = new Collector
    val digests = store.queryUpstreams(system.constraints, collector.collectUpstream)
    assertEquals(
      List(UpstreamPairChangeEvent(VersionID(pair, "id1"), AttributesUtil.toSeqFromTyped(system.includedAttrs), DEC_31_2009, "upstreamVsn-id1")),
      collector.upstreamObjs.toList)
  }

  @Theory
  def queryDownstreamRangeExcludesExcluded(system:AttributeSystem) = {
    val writer = store.openWriter()
    writer.storeDownstreamVersion(VersionID(pair, "id1"), system.includedAttrs, DEC_31_2009, "upstreamVsn-id1", "downstreamVsn-id1")
    writer.storeDownstreamVersion(VersionID(pair, "id2"), system.excludedAttrs, DEC_31_2009, "upstreamVsn-id2", "downstreamVsn-id1")
    writer.flush()

    val collector = new Collector
    val digests = store.queryDownstreams(system.constraints, collector.collectDownstream)
    assertEquals(
      List(DownstreamCorrelatedPairChangeEvent(VersionID(pair, "id1"), AttributesUtil.toSeqFromTyped(system.includedAttrs), DEC_31_2009, "upstreamVsn-id1", "downstreamVsn-id1")),
      collector.downstreamObjs.toList)
  }

  @Test
  def queryUpstreamRangeReturnsInIDOrder = {
    val writer = store.openWriter()
    writer.storeUpstreamVersion(VersionID(pair, "id7"), bizDateTimeMap(DEC_2_2009), DEC_2_2009, "upstreamVsn-id7")
    writer.storeUpstreamVersion(VersionID(pair, "id6"), bizDateTimeMap(DEC_1_2009), DEC_1_2009, "upstreamVsn-id6")
    writer.flush()

    val collector = new Collector
    val digests = store.queryUpstreams(List(unconstrainedDateTime("bizDate")), collector.collectUpstream)
    assertEquals(
      List(
        UpstreamPairChangeEvent(VersionID(pair, "id6"), bizDateTimeSeq(DEC_1_2009), DEC_1_2009, "upstreamVsn-id6"),
        UpstreamPairChangeEvent(VersionID(pair, "id7"), bizDateTimeSeq(DEC_2_2009), DEC_2_2009, "upstreamVsn-id7")),
      collector.upstreamObjs.toList)
  }

  @Test
  def queryDownstreamRangeReturnsInIDOrder = {
    val writer = store.openWriter()
    writer.storeDownstreamVersion(VersionID(pair, "id7"), bizDateTimeMap(DEC_2_2009), DEC_2_2009, "upstreamVsn-id7", "downstreamVsn-id7")
    writer.storeDownstreamVersion(VersionID(pair, "id6"), bizDateTimeMap(DEC_1_2009), DEC_1_2009, "upstreamVsn-id6", "downstreamVsn-id6")
    writer.flush()

    val collector = new Collector
    val digests = store.queryDownstreams(List(unconstrainedDateTime("bizDate")), collector.collectDownstream)
    assertEquals(
      List(
        DownstreamCorrelatedPairChangeEvent(VersionID(pair, "id6"), bizDateTimeSeq(DEC_1_2009), DEC_1_2009, "upstreamVsn-id6", "downstreamVsn-id6"),
        DownstreamCorrelatedPairChangeEvent(VersionID(pair, "id7"), bizDateTimeSeq(DEC_2_2009), DEC_2_2009, "upstreamVsn-id7", "downstreamVsn-id7")),
      collector.downstreamObjs.toList)
  }
  
  @Test
  def storedUpstreamShouldBeRetrievable = {
    val timestamp = new DateTime()
    val writer = store.openWriter()
    writer.storeUpstreamVersion(VersionID(pair, "id23"), emptyAttributes, DEC_1_2009, "upstreamVsn-id23")
    writer.flush()
    val corr = store.retrieveCurrentCorrelation(VersionID(pair, "id23")).getOrElse(null)

    assertCorrelationEquals(
      Correlation(null, pair, "id23", emptyStrAttributes, null, DEC_1_2009, timestamp, "upstreamVsn-id23", null, null, false),
      corr)
  }

  @Test
  def storedDownstreamShouldBeRetrievable = {
    val timestamp = new DateTime()
    val writer = store.openWriter()
    writer.storeDownstreamVersion(VersionID(pair, "id23"), emptyAttributes, DEC_1_2009, "upstreamVsn-id23", "downstreamVsn-id23")
    writer.flush()
    val corr = store.retrieveCurrentCorrelation(VersionID(pair, "id23")).getOrElse(null)

    assertCorrelationEquals(
      Correlation(null, pair, "id23", null, emptyStrAttributes, DEC_1_2009, timestamp, null, "upstreamVsn-id23", "downstreamVsn-id23", false),
      corr)
  }

  @Test
  def storedMatchShouldBeRetrievable = {
    val timestamp = new DateTime()
    val writer = store.openWriter()
    writer.storeUpstreamVersion(VersionID(pair, "id23"), emptyAttributes, DEC_1_2009, "upstreamVsn-id23")
    writer.storeDownstreamVersion(VersionID(pair, "id23"), emptyAttributes, DEC_1_2009, "upstreamVsn-id23", "downstreamVsn-id23")
    writer.flush()
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
    val writer = store.openWriter()
    val otherStore = stores(otherPair)
    val otherWriter = otherStore.openWriter()

    otherWriter.storeUpstreamVersion(VersionID(otherPair, "123456789"), emptyAttributes, DEC_1_2009, "up-123456789")
    otherWriter.storeDownstreamVersion(VersionID(otherPair, "123456789"), emptyAttributes, DEC_1_2009, "up-123456789", "down-123456789")
    otherWriter.flush()

    assertCorrelationEquals(
      Correlation(null, otherPair, "123456789", Map(), Map(), DEC_1_2009, null, "up-123456789", "up-123456789", "down-123456789", true),
      otherStore.retrieveCurrentCorrelation(VersionID(otherPair, "123456789")).getOrElse(null))

    writer.storeUpstreamVersion(VersionID(pair, "123456789"), emptyAttributes, DEC_1_2009, "up-987654321")
    writer.flush()
    assertCorrelationEquals(
      Correlation(null, pair, "123456789", Map(), Map(), DEC_1_2009, null, "up-987654321", null, null, false),
      store.retrieveCurrentCorrelation(VersionID(pair, "123456789")).getOrElse(null))

    // re-check other store
    assertCorrelationEquals(
      Correlation(null, otherPair, "123456789", Map(), Map(), DEC_1_2009, null, "up-123456789", "up-123456789", "down-123456789", true),
      otherStore.retrieveCurrentCorrelation(VersionID(otherPair, "123456789")).getOrElse(null))
  }

  @Test
  def flushingWriterMustClearBuffers {
    val writer = store.openWriter()
    assertFalse(writer.isDirty)
    writer.storeUpstreamVersion(VersionID(pair, "id23"), emptyAttributes, DEC_1_2009, "upstreamVsn-id23")
    assertTrue(writer.isDirty)
    writer.storeDownstreamVersion(VersionID(pair, "id23"), emptyAttributes, DEC_1_2009, "upstreamVsn-id23", "downstreamVsn-id23")
    assertTrue(writer.isDirty)
    writer.flush()
    assertFalse(writer.isDirty)
    writer.clearUpstreamVersion(VersionID(pair, "id23"))
    assertTrue(writer.isDirty)
    writer.flush()
    assertFalse(writer.isDirty)
  }

  @Test
  def writerMustFlushWhenMaxBufferSizeIsReached {
    val writer = store.openWriter()
    assertFalse(writer.isDirty)
    for (i <- 1 to 9999) {
      writer.storeUpstreamVersion(VersionID(pair, "id" + i), emptyAttributes, DEC_1_2009, "upstreamVsn-id" + i)
      assertTrue(writer.isDirty)
    }
    writer.storeUpstreamVersion(VersionID(pair, "id10000"), emptyAttributes, DEC_1_2009, "upstreamVsn-id10000")
    // should be flushed implicitly at this point
    assertFalse(writer.isDirty)
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
  def bizDateTimeSeq(d:DateTime) = Seq(d.toString())
  def bizDateTimeMap(d:DateTime) = Map("bizDateTime" -> DateTimeAttribute(d))
  def bizDateSeq(d:LocalDate) = Seq(d.toString())
  def bizDateMap(d:LocalDate) = Map("bizDate" -> DateAttribute(d))
  def intMap(i:Int) = Map("someInt" -> IntegerAttribute(i))
  def stringMap(s:String) = Map("someString" -> StringAttribute(s))

  // Standard attribute/constraint definitions
  private val dateTimeAttributes = bizDateTimeMap(JUL_1_2010_1)
  private val excludedByEarlierDateTimeAttributes = bizDateTimeMap(FEB_15_2010)
  private val excludedByLaterDateTimeAttributes = bizDateTimeMap(AUG_11_2010_1)
  private val dateTimeConstraints = Seq(DateTimeRangeConstraint("bizDateTime", JUL_2010, END_JUL_2010))
  private val noDateTimeConstraint = Seq(NoConstraint("bizDateTime"))

  private val dateAttributes = bizDateMap(JUL_1_2010.toLocalDate)
  private val excludedByEarlierDateAttributes = bizDateMap(FEB_15_2010.toLocalDate)
  private val excludedByLaterDateAttributes = bizDateMap(AUG_11_2010.toLocalDate)
  private val dateConstraints = Seq(DateRangeConstraint("bizDate", JUL_1_2010.toLocalDate, JUL_31_2010.toLocalDate))

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

  @DataPoints def dateTimes = Array(
    AttributeSystem(dateTimeConstraints, dateTimeAttributes, excludedByLaterDateTimeAttributes),
    AttributeSystem(dateTimeConstraints, dateTimeAttributes, excludedByEarlierDateTimeAttributes)
  )
  @DataPoints def dates = Array(
    AttributeSystem(dateConstraints, dateAttributes, excludedByLaterDateAttributes),
    AttributeSystem(dateConstraints, dateAttributes, excludedByEarlierDateAttributes)
  )
  @DataPoint def ints = AttributeSystem(intConstraints, intAttributes, excludedIntAttributes)
  @DataPoint def strings = AttributeSystem(stringConstraints, stringAttributes, excludedStringAttributes)
  @DataPoint def set = AttributeSystem(setConstraints, stringAttributes, excludedStringAttributes)
  @DataPoints def setAndDateTimes = Array(
    AttributeSystem(dateTimeConstraints ++ setConstraints, dateTimeAttributes ++ stringAttributes, excludedByLaterDateTimeAttributes ++ excludedStringAttributes),
    AttributeSystem(dateTimeConstraints ++ setConstraints, dateTimeAttributes ++ stringAttributes, dateTimeAttributes ++ excludedStringAttributes),
    AttributeSystem(dateTimeConstraints ++ setConstraints, dateTimeAttributes ++ stringAttributes, excludedByLaterDateTimeAttributes ++ stringAttributes)
  )

  def flushStore = {
    indexer.reset
  }
}