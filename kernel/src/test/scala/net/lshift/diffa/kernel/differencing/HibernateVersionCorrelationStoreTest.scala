/**
 * Copyright (C) 2010 LShift Ltd.
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
import scala.collection.Map
import collection.mutable.{HashMap, ListBuffer}

/**
 * Test cases for the Hibernate backed VersionCorrelationStore.
 */

class HibernateVersionCorrelationStoreTest {
  private val store = HibernateVersionCorrelationStoreTest.store

  private val pair = "pair"
  private val otherPair = "other-pair"
  private val categories = new HashMap[String,String]
  
  @Before
  def cleanupStore {
    val s = HibernateVersionCorrelationStoreTest.sessionFactory.openSession
    s.createQuery("delete from Correlation").executeUpdate
    s.flush
    s.close
  }

  @Test
  def matchedPairs = {
    //store.storeUpstreamVersion(VersionID(pair, "id1"), DEC_31_2009, DEC_31_2009, "upstreamVsn")
    store.storeUpstreamVersion(VersionID(pair, "id1"), categories, DEC_31_2009, "upstreamVsn")
    //store.storeDownstreamVersion(VersionID(pair, "id1"), DEC_31_2009, DEC_31_2009, "upstreamVsn", "downstreamVsn")
    store.storeDownstreamVersion(VersionID(pair, "id1"), categories, DEC_31_2009, "upstreamVsn", "downstreamVsn")

    val unmatched = store.unmatchedVersions(pair, List(DateRangeConstraint.any))
    assertEquals(0, unmatched.size)
  }

  @Test
  def unmatchedPairFromUpstream = {
    val timestamp = new DateTime()
    store.storeUpstreamVersion(VersionID(pair, "id2"), categories, DEC_31_2009, "upstreamVsn")
    //store.storeUpstreamVersion(VersionID(pair, "id2"), DEC_31_2009, DEC_31_2009, "upstreamVsn")

    val unmatched = store.unmatchedVersions(pair, List(DateRangeConstraint.any))
    assertEquals(1, unmatched.size)
    assertCorrelationEquals(Correlation(null, pair, "id2", categories, DEC_31_2009, timestamp, "upstreamVsn", null, null, false), unmatched(0))
    //assertCorrelationEquals(Correlation(null, pair, "id2", DEC_31_2009, DEC_31_2009, timestamp, "upstreamVsn", null, null, false), unmatched(0))
  }

  @Test
  def unmatchPairFromUpstreamShouldBeIndicatedInReturnValue {
    val timestamp = new DateTime()
    //val corr = store.storeUpstreamVersion(VersionID(pair, "id2"), DEC_31_2009, DEC_31_2009, "upstreamVsn")
    val corr = store.storeUpstreamVersion(VersionID(pair, "id2"), categories, DEC_31_2009, "upstreamVsn")
    assertCorrelationEquals(Correlation(null, pair, "id2", categories, DEC_31_2009, timestamp, "upstreamVsn", null, null, false), corr)
    //assertCorrelationEquals(Correlation(null, pair, "id2", DEC_31_2009, DEC_31_2009, timestamp, "upstreamVsn", null, null, false), corr)
  }

  @Test
  def unmatchedPairFromDownstream = {
    val timestamp = new DateTime()
    //store.storeDownstreamVersion(VersionID(pair, "id3"), DEC_31_2009, DEC_31_2009, "upstreamVsn", "downstreamVsn")
    store.storeDownstreamVersion(VersionID(pair, "id3"), categories, DEC_31_2009, "upstreamVsn", "downstreamVsn")

    val unmatched = store.unmatchedVersions(pair, List(DateRangeConstraint.any))
    assertEquals(1, unmatched.size)
    assertCorrelationEquals(Correlation(null, pair, "id3", categories, DEC_31_2009, timestamp,  null, "upstreamVsn", "downstreamVsn", false), unmatched(0))
    //assertCorrelationEquals(Correlation(null, pair, "id3", DEC_31_2009, DEC_31_2009, timestamp,  null, "upstreamVsn", "downstreamVsn", false), unmatched(0))
  }

  @Test
  def unmatchedPairFromDownstreamShouldBeIndicatedInReturnValue {
    val timestamp = new DateTime()
    val corr = store.storeDownstreamVersion(VersionID(pair, "id3"), categories, DEC_31_2009, "upstreamVsn", "downstreamVsn")
    //val corr = store.storeDownstreamVersion(VersionID(pair, "id3"), DEC_31_2009, DEC_31_2009, "upstreamVsn", "downstreamVsn")
    //assertCorrelationEquals(Correlation(null, pair, "id3", DEC_31_2009, DEC_31_2009, timestamp, null, "upstreamVsn", "downstreamVsn", false), corr)
    assertCorrelationEquals(Correlation(null, pair, "id3", categories, DEC_31_2009, timestamp, null, "upstreamVsn", "downstreamVsn", false), corr)
  }

  @Test
  def matchedPairsAfterChanges = {
    store.storeUpstreamVersion(VersionID(pair, "id4"), categories, DEC_31_2009, "upstreamVsnA")
    //store.storeUpstreamVersion(VersionID(pair, "id4"), DEC_31_2009, DEC_31_2009, "upstreamVsnA")
    store.storeUpstreamVersion(VersionID(pair, "id4"), categories, DEC_31_2009, "upstreamVsnB")
    //store.storeUpstreamVersion(VersionID(pair, "id4"), DEC_31_2009, DEC_31_2009, "upstreamVsnB")
    store.storeDownstreamVersion(VersionID(pair, "id4"), categories, DEC_31_2009, "upstreamVsnA", "downstreamVsnA")
    //store.storeDownstreamVersion(VersionID(pair, "id4"), DEC_31_2009, DEC_31_2009, "upstreamVsnA", "downstreamVsnA")
    store.storeDownstreamVersion(VersionID(pair, "id4"), categories, DEC_31_2009, "upstreamVsnB", "downstreamVsnB")
    //store.storeDownstreamVersion(VersionID(pair, "id4"), DEC_31_2009, DEC_31_2009, "upstreamVsnB", "downstreamVsnB")

    val unmatched = store.unmatchedVersions(pair, List(DateRangeConstraint.any))
    assertEquals(0, unmatched.size)
  }

  @Test
  def unmatchedPairsAfterChanges = {
    val timestamp = new DateTime()
    store.storeUpstreamVersion(VersionID(pair, "id5"), categories,DEC_31_2009, "upstreamVsnA")
    //store.storeUpstreamVersion(VersionID(pair, "id5"), DEC_31_2009,DEC_31_2009, "upstreamVsnA")
    store.storeDownstreamVersion(VersionID(pair, "id5"), categories, DEC_31_2009, "upstreamVsnA", "downstreamVsnA")
    //store.storeDownstreamVersion(VersionID(pair, "id5"), DEC_31_2009, DEC_31_2009, "upstreamVsnA", "downstreamVsnA")
    store.storeUpstreamVersion(VersionID(pair, "id5"), categories, DEC_31_2009, "upstreamVsnB")
    //store.storeUpstreamVersion(VersionID(pair, "id5"), DEC_31_2009, DEC_31_2009, "upstreamVsnB")

    val unmatched = store.unmatchedVersions(pair, List(DateRangeConstraint.any))
    assertEquals(1, unmatched.size)
    assertCorrelationEquals(Correlation(null, pair, "id5", categories, DEC_31_2009, timestamp, "upstreamVsnB", "upstreamVsnA", "downstreamVsnA", false), unmatched(0))
    //assertCorrelationEquals(Correlation(null, pair, "id5", DEC_31_2009, DEC_31_2009, timestamp, "upstreamVsnB", "upstreamVsnA", "downstreamVsnA", false), unmatched(0))
  }

  @Test
  def unmatchedPairsAfterChangesShouldBeIndicatedInReturnValue = {
    val timestamp = new DateTime()
    //store.storeUpstreamVersion(VersionID(pair, "id5"), DEC_31_2009, DEC_31_2009, "upstreamVsnA")
    store.storeUpstreamVersion(VersionID(pair, "id5"), categories, DEC_31_2009, "upstreamVsnA")
    store.storeDownstreamVersion(VersionID(pair, "id5"), categories, DEC_31_2009, "upstreamVsnA", "downstreamVsnA")
    //store.storeDownstreamVersion(VersionID(pair, "id5"), DEC_31_2009, DEC_31_2009, "upstreamVsnA", "downstreamVsnA")
    //val corr = store.storeUpstreamVersion(VersionID(pair, "id5"), DEC_31_2009, DEC_31_2009, "upstreamVsnB")
    val corr = store.storeUpstreamVersion(VersionID(pair, "id5"), categories, DEC_31_2009, "upstreamVsnB")

    assertCorrelationEquals(Correlation(null, pair, "id5", categories, DEC_31_2009, timestamp, "upstreamVsnB", "upstreamVsnA", "downstreamVsnA", false), corr)
    //assertCorrelationEquals(Correlation(null, pair, "id5", DEC_31_2009, DEC_31_2009, timestamp, "upstreamVsnB", "upstreamVsnA", "downstreamVsnA", false), corr)
  }

  @Test
  def deletingSource = {
    store.storeUpstreamVersion(VersionID(pair, "id6"), categories, DEC_1_2009, "upstreamVsn-id6")
    //store.storeUpstreamVersion(VersionID(pair, "id6"), DEC_1_2009, DEC_1_2009, "upstreamVsn-id6")
    store.storeUpstreamVersion(VersionID(pair, "id7"), categories,DEC_1_2009, "upstreamVsn-id7")
    //store.storeUpstreamVersion(VersionID(pair, "id7"), DEC_1_2009,DEC_1_2009, "upstreamVsn-id7")
    val corr = store.clearUpstreamVersion(VersionID(pair, "id6"))

    assertCorrelationEquals(Correlation(null, pair, "id6", null, null, null, null, null, null, true), corr)

    val collector = new Collector
    store.queryUpstreams(pair, List(DateRangeConstraint(DEC_1_2009, endOfDay(DEC_1_2009))), collector.collectUpstream)
    assertEquals(
      List(UpstreamPairChangeEvent(VersionID(pair, "id7"), categories, DEC_1_2009, "upstreamVsn-id7")),
      //List(UpstreamPairChangeEvent(VersionID(pair, "id7"), DEC_1_2009, DEC_1_2009, "upstreamVsn-id7")),
      collector.upstreamObjs.toList)
  }

  @Test
  def deletingSourceThatIsMatched = {
    store.storeUpstreamVersion(VersionID(pair, "id6"), categories, DEC_1_2009, "upstreamVsn-id6")
    //store.storeUpstreamVersion(VersionID(pair, "id6"), DEC_1_2009, DEC_1_2009, "upstreamVsn-id6")
    store.storeDownstreamVersion(VersionID(pair, "id6"), categories, DEC_1_2009, "upstreamVsn-id6", "downstreamVsn-id6")
    //store.storeDownstreamVersion(VersionID(pair, "id6"), DEC_1_2009, DEC_1_2009, "upstreamVsn-id6", "downstreamVsn-id6")
    store.clearUpstreamVersion(VersionID(pair, "id6"))

    val collector = new Collector
    store.queryUpstreams(pair, List(DateRangeConstraint(DEC_1_2009, endOfDay(DEC_1_2009))), collector.collectUpstream)
    assertEquals(0, collector.upstreamObjs.size)
  }

  @Test
  def deletingDest = {
    store.storeDownstreamVersion(VersionID(pair, "id6"), categories, DEC_1_2009, "upstreamVsn-id6", "downstreamVsn-id6")
    //store.storeDownstreamVersion(VersionID(pair, "id6"), DEC_1_2009, DEC_1_2009, "upstreamVsn-id6", "downstreamVsn-id6")
    store.storeDownstreamVersion(VersionID(pair, "id7"), categories, DEC_1_2009, "upstreamVsn-id7", "downstreamVsn-id7")
    //store.storeDownstreamVersion(VersionID(pair, "id7"), DEC_1_2009, DEC_1_2009, "upstreamVsn-id7", "downstreamVsn-id7")
    val corr = store.clearDownstreamVersion(VersionID(pair, "id6"))

    assertCorrelationEquals(Correlation(null, pair, "id6", null, null, null, null, null, null, true), corr)

    val collector = new Collector
    val digests = store.queryDownstreams(pair, List(DateRangeConstraint(DEC_1_2009, endOfDay(DEC_1_2009))), collector.collectDownstream)
    assertEquals(
      List(DownstreamCorrelatedPairChangeEvent(VersionID(pair, "id7"), categories, DEC_1_2009, "upstreamVsn-id7", "downstreamVsn-id7")),
      //List(DownstreamCorrelatedPairChangeEvent(VersionID(pair, "id7"), DEC_1_2009, DEC_1_2009, "upstreamVsn-id7", "downstreamVsn-id7")),
      collector.downstreamObjs.toList)
  }

  @Test
  def deletingDestThatIsMatched = {
    store.storeUpstreamVersion(VersionID(pair, "id6"), categories, DEC_1_2009, "upstreamVsn-id6")
    //store.storeUpstreamVersion(VersionID(pair, "id6"), DEC_1_2009, DEC_1_2009, "upstreamVsn-id6")
    store.storeDownstreamVersion(VersionID(pair, "id6"), categories, DEC_1_2009, "upstreamVsn-id6", "downstreamVsn-id6")
    //store.storeDownstreamVersion(VersionID(pair, "id6"), DEC_1_2009, DEC_1_2009, "upstreamVsn-id6", "downstreamVsn-id6")
    store.clearDownstreamVersion(VersionID(pair, "id6"))

    val collector = new Collector
    store.queryDownstreams(pair, List(DateRangeConstraint(DEC_1_2009, endOfDay(DEC_1_2009))), collector.collectDownstream)
    assertEquals(0, collector.downstreamObjs.size)
  }

  @Test
  def queryUpstreamRangeExcludesEarlier = {
    store.storeUpstreamVersion(VersionID(pair, "id6"), categories, DEC_1_2009, "upstreamVsn-id6")
    //store.storeUpstreamVersion(VersionID(pair, "id6"), DEC_1_2009, DEC_1_2009, "upstreamVsn-id6")
    store.storeUpstreamVersion(VersionID(pair, "id7"), categories, DEC_2_2009, "upstreamVsn-id7")
    //store.storeUpstreamVersion(VersionID(pair, "id7"), DEC_2_2009, DEC_2_2009, "upstreamVsn-id7")

    val collector = new Collector
    val digests = store.queryUpstreams(pair, List(DateRangeConstraint(DEC_2_2009, endOfDay(DEC_2_2009))), collector.collectUpstream)
    assertEquals(
      List(UpstreamPairChangeEvent(VersionID(pair, "id7"), categories, DEC_2_2009, "upstreamVsn-id7")),
      //List(UpstreamPairChangeEvent(VersionID(pair, "id7"), DEC_2_2009, DEC_2_2009, "upstreamVsn-id7")),
      collector.upstreamObjs.toList)
  }

  @Test
  def queryUpstreamRangeExcludesLater = {
    store.storeUpstreamVersion(VersionID(pair, "id6"), categories,DEC_1_2009, "upstreamVsn-id6")
    //store.storeUpstreamVersion(VersionID(pair, "id6"), DEC_1_2009,DEC_1_2009, "upstreamVsn-id6")
    store.storeUpstreamVersion(VersionID(pair, "id7"), categories,DEC_2_2009, "upstreamVsn-id7")
    //store.storeUpstreamVersion(VersionID(pair, "id7"), DEC_2_2009,DEC_2_2009, "upstreamVsn-id7")

    val collector = new Collector
    val digests = store.queryUpstreams(pair, List(DateRangeConstraint(DEC_1_2009, endOfDay(DEC_1_2009))), collector.collectUpstream)
    assertEquals(
      //List(UpstreamPairChangeEvent(VersionID(pair, "id6"), DEC_1_2009, DEC_1_2009, "upstreamVsn-id6")),
      List(UpstreamPairChangeEvent(VersionID(pair, "id6"), categories, DEC_1_2009, "upstreamVsn-id6")),
      collector.upstreamObjs.toList)
  }

  @Test
  def queryDownstreamRangeExcludesEarlier = {
    store.storeDownstreamVersion(VersionID(pair, "id6"), categories, DEC_1_2009, "upstreamVsn-id6", "downstreamVsn-id6")
    //store.storeDownstreamVersion(VersionID(pair, "id6"), DEC_1_2009, DEC_1_2009, "upstreamVsn-id6", "downstreamVsn-id6")
    store.storeDownstreamVersion(VersionID(pair, "id7"), categories, DEC_2_2009, "upstreamVsn-id7", "downstreamVsn-id7")
    //store.storeDownstreamVersion(VersionID(pair, "id7"), DEC_2_2009, DEC_2_2009, "upstreamVsn-id7", "downstreamVsn-id7")

    val collector = new Collector
    val digests = store.queryDownstreams(pair, List(DateRangeConstraint(DEC_2_2009, endOfDay(DEC_2_2009))), collector.collectDownstream)
    assertEquals(
      //List(DownstreamCorrelatedPairChangeEvent(VersionID(pair, "id7"), DEC_2_2009, DEC_2_2009, "upstreamVsn-id7", "downstreamVsn-id7")),
      List(DownstreamCorrelatedPairChangeEvent(VersionID(pair, "id7"), categories, DEC_2_2009, "upstreamVsn-id7", "downstreamVsn-id7")),
      collector.downstreamObjs.toList)
  }

  @Test
  def queryDownstreamRangeExcludesLater = {
    store.storeDownstreamVersion(VersionID(pair, "id6"), categories, DEC_1_2009, "upstreamVsn-id6", "downstreamVsn-id6")
    //store.storeDownstreamVersion(VersionID(pair, "id6"), DEC_1_2009, DEC_1_2009, "upstreamVsn-id6", "downstreamVsn-id6")
    store.storeDownstreamVersion(VersionID(pair, "id7"), categories, DEC_2_2009, "upstreamVsn-id7", "downstreamVsn-id7")
    //store.storeDownstreamVersion(VersionID(pair, "id7"), DEC_2_2009, DEC_2_2009, "upstreamVsn-id7", "downstreamVsn-id7")

    val collector = new Collector
    val digests = store.queryDownstreams(pair, List(DateRangeConstraint(DEC_1_2009, endOfDay(DEC_1_2009))), collector.collectDownstream)
    assertEquals(
      //List(DownstreamCorrelatedPairChangeEvent(VersionID(pair, "id6"), DEC_1_2009, DEC_1_2009, "upstreamVsn-id6", "downstreamVsn-id6")),
      List(DownstreamCorrelatedPairChangeEvent(VersionID(pair, "id6"), categories, DEC_1_2009, "upstreamVsn-id6", "downstreamVsn-id6")),
      collector.downstreamObjs.toList)
  }
  
  @Test
  def storedUpstreamShouldBeRetrievable = {
    val timestamp = new DateTime()
    store.storeUpstreamVersion(VersionID(pair, "id23"), categories, DEC_1_2009, "upstreamVsn-id23")
    //store.storeUpstreamVersion(VersionID(pair, "id23"), DEC_1_2009, DEC_1_2009, "upstreamVsn-id23")
    val corr = store.retrieveCurrentCorrelation(VersionID(pair, "id23")).getOrElse(null)

    assertCorrelationEquals(
      Correlation(null, pair, "id23", categories, DEC_1_2009, timestamp, "upstreamVsn-id23", null, null, false),
      //Correlation(null, pair, "id23", DEC_1_2009, DEC_1_2009, timestamp, "upstreamVsn-id23", null, null, false),
      corr)
  }

  @Test
  def storedDownstreamShouldBeRetrievable = {
    val timestamp = new DateTime()
    store.storeDownstreamVersion(VersionID(pair, "id23"), categories, DEC_1_2009, "upstreamVsn-id23", "downstreamVsn-id23")
    //store.storeDownstreamVersion(VersionID(pair, "id23"), DEC_1_2009, DEC_1_2009, "upstreamVsn-id23", "downstreamVsn-id23")
    val corr = store.retrieveCurrentCorrelation(VersionID(pair, "id23")).getOrElse(null)

    assertCorrelationEquals(
      Correlation(null, pair, "id23", categories, DEC_1_2009, timestamp, null, "upstreamVsn-id23", "downstreamVsn-id23", false),
      //Correlation(null, pair, "id23", DEC_1_2009, DEC_1_2009, timestamp, null, "upstreamVsn-id23", "downstreamVsn-id23", false),
      corr)
  }

  @Test
  def storedMatchShouldBeRetrievable = {
    val timestamp = new DateTime()
    store.storeUpstreamVersion(VersionID(pair, "id23"), categories, DEC_1_2009, "upstreamVsn-id23")
    //store.storeUpstreamVersion(VersionID(pair, "id23"), DEC_1_2009, DEC_1_2009, "upstreamVsn-id23")
    store.storeDownstreamVersion(VersionID(pair, "id23"), categories, DEC_1_2009, "upstreamVsn-id23", "downstreamVsn-id23")
    //store.storeDownstreamVersion(VersionID(pair, "id23"), DEC_1_2009, DEC_1_2009, "upstreamVsn-id23", "downstreamVsn-id23")
    val corr = store.retrieveCurrentCorrelation(VersionID(pair, "id23")).getOrElse(null)

    assertCorrelationEquals(
      Correlation(null, pair, "id23", categories, DEC_1_2009, timestamp, "upstreamVsn-id23", "upstreamVsn-id23", "downstreamVsn-id23", true),
      //Correlation(null, pair, "id23", DEC_1_2009, DEC_1_2009, timestamp, "upstreamVsn-id23", "upstreamVsn-id23", "downstreamVsn-id23", true),
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
      assertEquals(expected.categories, actual.categories)
    }
  }
}

class Collector {
  val upstreamObjs = new ListBuffer[UpstreamPairChangeEvent]
  val downstreamObjs = new ListBuffer[DownstreamCorrelatedPairChangeEvent]

  def collectUpstream(id:VersionID, categories:Map[String,String], lastUpdate:DateTime, vsn:String) = {
    upstreamObjs += UpstreamPairChangeEvent(id, categories, lastUpdate, vsn)
  }
  def collectDownstream(id:VersionID, categories:Map[String,String], lastUpdate:DateTime, uvsn:String, dvsn:String) = {
    downstreamObjs += DownstreamCorrelatedPairChangeEvent(id, categories, lastUpdate, uvsn, dvsn)
  }
}

object HibernateVersionCorrelationStoreTest {
  private val config = new Configuration().
          addResource("net/lshift/diffa/kernel/differencing/Correlations.hbm.xml").
          setProperty("hibernate.dialect", "org.hibernate.dialect.DerbyDialect").
          setProperty("hibernate.connection.url", "jdbc:derby:target/versionStore;create=true").
          setProperty("hibernate.connection.driver_class", "org.apache.derby.jdbc.EmbeddedDriver").
          setProperty("hibernate.hbm2ddl.auto", "create-drop")

  val sessionFactory = config.buildSessionFactory
  val store = new HibernateVersionCorrelationStore(sessionFactory)
}