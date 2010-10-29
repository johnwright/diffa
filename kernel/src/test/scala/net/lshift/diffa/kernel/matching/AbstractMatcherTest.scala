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

package net.lshift.diffa.kernel.matching

import org.junit.{Test, After, Before}
import concurrent.{TIMEOUT, MailBox}
import net.lshift.diffa.kernel.events.{DownstreamPairChangeEvent, UpstreamPairChangeEvent, VersionID}
import org.joda.time.DateTime
import org.junit.Assert._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import collection.mutable.HashMap

/**
 * Standard tests for a matcher.
 */
abstract class AbstractMatcherTest {
  var analyser:EventMatcher = null
  var slowAnalyser:EventMatcher = null
  val ackCallbackAListener = new MailBox
  val ackCallbackBListener = new MailBox

  val pairId = "pair" + AbstractMatcherTest.nextPairId

  val ackCallbackA = () => ackCallbackAListener.send(new Object)
  val ackCallbackB = () => ackCallbackBListener.send(new Object)

  val id1 = VersionID(pairId, "aaaa1")
  val id2 = VersionID(pairId, "aaaa2")
  val id3 = VersionID(pairId, "aaaa3")
  val id4 = VersionID(pairId, "aaaa4")
  val id5 = VersionID(pairId, "aaaa5")
  val id6 = VersionID(pairId, "aaaa6")
  val idB6 = VersionID(pairId, "bbbb6")
  val id7 = VersionID(pairId, "aaaa7")

  def createMatcher(id:String, timeout:Int):EventMatcher

  @Before
  def createAnalyser {
    analyser = createMatcher(pairId, 1)
    slowAnalyser = createMatcher(pairId + "-Slow", 3)
  }

  @After
  def disposeAnalyser {
    analyser.dispose
    slowAnalyser.dispose
  }

  @Test
  def addingBalancedPairShouldResultInPairedMessageAndAcksBeingTriggered {
    val mb = new MailBox
    analyser.addListener(new MatchingStatusListener {
      def onUpstreamExpired(id:VersionID, vsn:String) = null
      def onPaired(id: VersionID, vsn:String) {
        mb.send(new Object)
      }
      def onDownstreamExpired(id: VersionID, vsn:String) = null
    })

    analyser.onChange(new UpstreamPairChangeEvent(id1, new HashMap[String,String], new DateTime, "vsnAAAA"), ackCallbackA)
    analyser.onChange(new DownstreamPairChangeEvent(id1, new HashMap[String,String], new DateTime, "vsnAAAA"), ackCallbackB)

    mb.receiveWithin(1000) {
      case TIMEOUT => fail("Failed to recieve a matched pair event")
      case _ =>
    }
    ackCallbackAListener.receiveWithin(100) {
      case TIMEOUT => fail("Ack Callback for upstream not triggered")
      case _       =>
    }
    ackCallbackBListener.receiveWithin(100) {
      case TIMEOUT => fail("Ack Callback for downstream not triggered")
      case _       =>
    }
  }

  @Test
  def addingSeriesOfBalancedPairsShouldResultInPairedMessages {
    val mb = new MailBox
    analyser.addListener(new MatchingStatusListener {
      def onUpstreamExpired(id: VersionID, vsn:String) = null
      def onPaired(id: VersionID, vsn:String) {
        mb.send(id)
      }
      def onDownstreamExpired(id: VersionID, vsn:String) = null
    })

    analyser.onChange(new UpstreamPairChangeEvent(id1, new HashMap[String,String], new DateTime, "vsnAAAA"), ackCallbackA)
    analyser.onChange(new DownstreamPairChangeEvent(id1, new HashMap[String,String], new DateTime, "vsnAAAA"), ackCallbackB)
    analyser.onChange(new UpstreamPairChangeEvent(id2, new HashMap[String,String], new DateTime, "vsnAAAB"), ackCallbackA)
    analyser.onChange(new DownstreamPairChangeEvent(id2, new HashMap[String,String], new DateTime, "vsnAAAB"), ackCallbackB)

    mb.receiveWithin(1000) {
      case TIMEOUT => fail("Failed to recieve first matched pair event")
      case id => assertEquals(id1, id)
    }
    mb.receiveWithin(10) {
      case TIMEOUT => fail("Failed to recieve second matched pair event")
      case id => assertEquals(id2, id)
    }
  }

  @Test
  def addingUnbalancedUpstreamShouldResultInUpstreamExpiredMessageAndUpstreamAckCalled {
    val mb = new MailBox
    analyser.addListener(new MatchingStatusListener {
      def onUpstreamExpired(id: VersionID, vsn:String) = {
        mb.send(id)
      }
      def onPaired(id: VersionID, vsn:String) = null
      def onDownstreamExpired(id: VersionID, vsn:String) = null
    })

    analyser.onChange(new UpstreamPairChangeEvent(id2, new HashMap[String,String], new DateTime, "vsnAAAA"), ackCallbackA)

    mb.receiveWithin(10000) {
      case TIMEOUT => fail("Failed to recieve a expired upstream event")
      case id:VersionID => assertEquals(id2, id)
    }
    ackCallbackAListener.receiveWithin(100) {
      case TIMEOUT => fail("Ack Callback for upstream not triggered")
      case _       =>
    }
  }

  @Test
  def addingSeriesOfUnbalancedUpstreamsShouldResultInUpstreamExpiredMessages {
    val mb = new MailBox
    analyser.addListener(new MatchingStatusListener {
      def onUpstreamExpired(id: VersionID, vsn:String) = {
        mb.send(id)
      }
      def onPaired(id: VersionID, vsn:String) = null
      def onDownstreamExpired(id: VersionID, vsn:String) = null
    })

    analyser.onChange(new UpstreamPairChangeEvent(id2, new HashMap[String,String], new DateTime, "vsnAAAA"), ackCallbackA)
    analyser.onChange(new UpstreamPairChangeEvent(id3, new HashMap[String,String], new DateTime, "vsnAAAB"), ackCallbackA)
    analyser.onChange(new UpstreamPairChangeEvent(id4, new HashMap[String,String], new DateTime, "vsnAAAC"), ackCallbackA)

    mb.receiveWithin(10000) {
      case TIMEOUT => fail("Failed to recieve first expired upstream event")
      case id => assertEquals(id2, id)
    }
    mb.receiveWithin(1000) {
      case TIMEOUT => fail("Failed to recieve second expired upstream event")
      case id => assertEquals(id3, id)
    }
    mb.receiveWithin(1000) {
      case TIMEOUT => fail("Failed to recieve third expired upstream event")
      case id => assertEquals(id4, id)
    }
  }

  @Test
  def addingUnbalancedDownstreamShouldResultInDownstreamExpiredMessageAndDownstreamAckCalled {
    val mb = new MailBox
    analyser.addListener(new MatchingStatusListener {
      def onUpstreamExpired(id: VersionID, vsn:String) {}
      def onPaired(id: VersionID, vsn:String) {}
      def onDownstreamExpired(id: VersionID, vsn:String) = {
        mb.send(id)
      }
    })

    analyser.onChange(new DownstreamPairChangeEvent(id3, new HashMap[String,String], new DateTime, "vsnAAAA"), ackCallbackB)

    mb.receiveWithin(2000) {
      case TIMEOUT => fail("Failed to recieve a expired downstream event")
      case id:VersionID => assertEquals(id3, id)
    }
    ackCallbackBListener.receiveWithin(100) {
      case TIMEOUT => fail("Ack Callback for downstream not triggered")
      case _       =>
    }
  }

  @Test
  def addingBalancedPairShouldNotResultInUpstreamExpiredMessage {
    val mb = new MailBox
    slowAnalyser.addListener(new MatchingStatusListener {
      def onUpstreamExpired(id: VersionID, vsn:String) = {
        mb.send(new Object)
      }
      def onPaired(id: VersionID, vsn:String) = null
      def onDownstreamExpired(id: VersionID, vsn:String) = null
    })

    slowAnalyser.onChange(new DownstreamPairChangeEvent(id4, new HashMap[String,String], new DateTime, "vsnAAAA"), ackCallbackB)
    Thread.sleep(1000)
    slowAnalyser.onChange(new UpstreamPairChangeEvent(id4, new HashMap[String,String], new DateTime, "vsnAAAA"), ackCallbackA)

    mb.receiveWithin(5000) {
      case TIMEOUT =>
      case _ => fail("Should not have received an expired upstream event")
    }
  }

  @Test
  def addingBalancedPairShouldNotResultInDownstreamExpiredMessage {
    val mb = new MailBox
    slowAnalyser.addListener(new MatchingStatusListener {
      def onUpstreamExpired(id: VersionID, vsn:String) {}
      def onPaired(id: VersionID, vsn:String) = null
      def onDownstreamExpired(id: VersionID, vsn:String) = {
        mb.send(new Object)
      }
    })

    slowAnalyser.onChange(new UpstreamPairChangeEvent(id5, new HashMap[String,String], new DateTime, "vsnAAAA"), ackCallbackA)
    Thread.sleep(1000)
    slowAnalyser.onChange(new DownstreamPairChangeEvent(id5, new HashMap[String,String], new DateTime, "vsnAAAA"), ackCallbackB)

    mb.receiveWithin(5000) {
      case TIMEOUT =>
      case _ => fail("Should not have received an expired downstream event")
    }
  }

  @Test
  def addingPairWithDifferentIdsShouldNotResultInPairedMessage {
    val mb = new MailBox
    analyser.addListener(new MatchingStatusListener {
      def onUpstreamExpired(id: VersionID, vsn:String) = null
      def onPaired(id: VersionID, vsn:String) {
        mb.send(new Object)
      }
      def onDownstreamExpired(id: VersionID, vsn:String) = null
    })

    analyser.onChange(new UpstreamPairChangeEvent(id6, new HashMap[String,String], new DateTime, "vsnAAAA"), ackCallbackA)
    analyser.onChange(new DownstreamPairChangeEvent(idB6, new HashMap[String,String], new DateTime, "vsnAAAA"), ackCallbackB)

    mb.receiveWithin(2000) {
      case TIMEOUT =>
      case _ => fail("Should not receive paired event")
    }
  }

  @Test
  def addingPairWithDifferentVersionsShouldNotResultInPairedMessage {
    val mb = new MailBox
    analyser.addListener(new MatchingStatusListener {
      def onUpstreamExpired(id: VersionID, vsn:String) = null
      def onPaired(id: VersionID, vsn:String) {
        mb.send(new Object)
      }
      def onDownstreamExpired(id: VersionID, vsn:String) = null
    })

    analyser.onChange(new UpstreamPairChangeEvent(id7, new HashMap[String,String], new DateTime, "vsnAAAA"), ackCallbackA)
    analyser.onChange(new DownstreamPairChangeEvent(id7, new HashMap[String,String], new DateTime, "vsnBBBB"), ackCallbackB)

    mb.receiveWithin(2000) {
      case TIMEOUT =>
      case _ => fail("Should not receive paired event")
    }
  }

  @Test
  def addedUpstreamObjectsShouldBeMarkedAsInProgressIfTheyArentMatchedOrExpired {
    analyser.onChange(new UpstreamPairChangeEvent(id1, new HashMap[String,String], new DateTime,"vsnAAAA"), ackCallbackA)
    assertTrue(analyser.isVersionIDActive(id1))
  }

  @Test
  def multipleAddedUpstreamObjectsShouldBeMarkedAsInProgressIfOnlySomeAreMatchedOrExpired {
    analyser.onChange(new UpstreamPairChangeEvent(id1, new HashMap[String,String], new DateTime, "vsnAAAA"), ackCallbackA)
    analyser.onChange(new UpstreamPairChangeEvent(id1, new HashMap[String,String], new DateTime, "vsnAAAB"), ackCallbackA)
    analyser.onChange(new DownstreamPairChangeEvent(id1, new HashMap[String,String], new DateTime, "vsnAAAA"), ackCallbackA)
    assertTrue(analyser.isVersionIDActive(id1))
  }


  @Test
  def addedDownstreamObjectsShouldBeMarkedAsInProgressIfTheyArentMatchedOrExpired {
    analyser.onChange(new DownstreamPairChangeEvent(id1, new HashMap[String,String], new DateTime, "vsnAAAA"), ackCallbackA)
    assertTrue(analyser.isVersionIDActive(id1))
  }

  @Test
  def addedObjectsShouldNotBeMarkedAsInProgressIfTheyAreMatched {
    analyser.onChange(new UpstreamPairChangeEvent(id1, new HashMap[String,String], new DateTime, "vsnAAAA"), ackCallbackA)
    analyser.onChange(new DownstreamPairChangeEvent(id1, new HashMap[String,String], new DateTime, "vsnAAAA"), ackCallbackB)
    assertFalse(analyser.isVersionIDActive(id1))
  }

  @Test
  def addedUpstreamObjectsShouldNotBeMarkedAsInProgressIfTheyHaveExpired {
    val mb = new MailBox
    analyser.addListener(new MatchingStatusListener {
      def onUpstreamExpired(id: VersionID, vsn:String) = mb.send(new Object)
      def onPaired(id: VersionID, vsn:String) = null
      def onDownstreamExpired(id: VersionID, vsn:String) = null
    })
    analyser.onChange(new UpstreamPairChangeEvent(id7, new HashMap[String,String], new DateTime, "vsnAAAA"), ackCallbackA)

    mb.receiveWithin(5000) {
      case TIMEOUT => fail("Should have received expiry event")
      case _ =>
    }
    assertFalse(analyser.isVersionIDActive(id7))
  }

  @Test
  def addedDownstreamObjectsShouldNotBeMarkedAsInProgressIfTheyHaveExpired {
    val mb = new MailBox
    analyser.addListener(new MatchingStatusListener {
      def onUpstreamExpired(id: VersionID, vsn:String) = null
      def onPaired(id: VersionID, vsn:String) = null
      def onDownstreamExpired(id: VersionID, vsn:String) = mb.send(new Object)
    })

    analyser.onChange(new DownstreamPairChangeEvent(id7, new HashMap[String,String], new DateTime, "vsnAAAA"), ackCallbackA)

    mb.receiveWithin(2000) {
      case TIMEOUT => fail("Should have received expiry event")
      case _ =>
    }
    assertFalse(analyser.isVersionIDActive(id7))
  }
}
object AbstractMatcherTest {
  private val counter = new AtomicInteger(1)

  def nextPairId = counter.getAndIncrement
}