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

package net.lshift.diffa.agent.itest

import org.joda.time.DateTime
import org.junit.Assert._
import net.lshift.diffa.agent.itest.support.TestConstants._
import java.lang.String
import support.TestEnvironment
import net.lshift.diffa.kernel.differencing.{SessionScope, SessionEvent}
import javax.mail.Session
import java.io.{File, FileInputStream}
import javax.mail.internet.MimeMessage
import util.matching.Regex
import org.slf4j.{Logger, LoggerFactory}
import java.net.URI
import org.junit.{Before, Test}
import net.lshift.diffa.kernel.participants.ParticipantType
import java.util.{UUID, Properties}

/**
 * Tests that can be applied to an environment to validate that differencing functionality works appropriately.
 */
trait CommonDifferenceTests {

  val log:Logger = LoggerFactory.getLogger(getClass)

  val mailDir = System.getProperty("diffa.maildir")
  log.debug("Using maildir: " + mailDir)
  
  val messageDir = (new File(mailDir)).getAbsoluteFile

  /**
   * The environment under test.
   */
  def env:TestEnvironment

  @Before
  def deleteMails() = {
    if (messageDir.exists) {
      messageDir.listFiles.foreach(f => f.delete)
    }
  }

  def getReport(pair:String, from:DateTime, until:DateTime) : Array[SessionEvent]= {
    var sessionId = env.diffClient.subscribe(SessionScope.forPairs(env.pairKey), yearAgo, today)
    env.diffClient.runSync(sessionId)

      // TODO: Ideally, the server should be able to tell us (somehow) when it has completed the initial sync.
      //       Since for now it can't, we'll just have to give it a little bit of time.
    Thread.sleep(1000)
    env.diffClient.poll(sessionId)
  }

  @Test
  def shouldFindNoDifferencesInEmptyParticipants {
    val diffs = getReport(env.pairKey, yearAgo, today)
    assertNotNull(diffs)
    assertTrue(diffs.isEmpty)
  }

  @Test
  def detectionTimeShouldBeMatchTheMostRecentUpdatedTimeOnAParticipatingEntity = {
    val then = new DateTime()
    val (diffs,_) = getVerifiedDiffsWithSessionId()
    assertNotNull(diffs(0))
    val detectionTime = diffs(0).detectedAt
    // TODO On a rainy day, look into why using a millisecond comparison is necessary
    assertEquals(yesterday.getMillis, detectionTime.getMillis)
  }

  @Test
  def shouldFindDifferencesInDifferingParticipants {
    val (diffs, sessionId) = getVerifiedDiffsWithSessionId()
    val seqId = diffs(0).seqId

    val detail = env.diffClient.eventDetail(sessionId, seqId, ParticipantType.UPSTREAM)
    assertNotNull(diffs)

    assertEquals("abcdef", detail)

    val fileList = messageDir.listFiles
    assertNotNull("File list was null for dir: " + messageDir, fileList)
    assertEquals(1, fileList.size)
    testForLink(fileList(0))
  }

  @Test
  def shouldFindDifferencesInParticipantsThatBecomeDifferent {
    var sessionId = env.diffClient.subscribe(SessionScope.forPairs(env.pairKey), yearAgo, today)
    env.diffClient.runSync(sessionId)
    Thread.sleep(100)
    env.addAndNotifyUpstream("abc", env.bizDate(yesterday), "abcdef")

    val diffs = tryAgain(sessionId,20,100)

    assertNotNull(diffs)
    assertFalse(diffs.isEmpty)
  }

  @Test
  def walkThroughEventDetails = {
    def guid() = UUID.randomUUID.toString
    val up = guid()
    val down = guid()
    val NO_CONTENT = "Expanded detail not available"
    var sessionId = env.diffClient.subscribe(SessionScope.forPairs(env.pairKey), yearAgo, today)
    env.diffClient.runSync(sessionId)
    Thread.sleep(100)
    env.addAndNotifyUpstream("abc", env.bizDate(yesterday), up)

    val diffs = tryAgain(sessionId,20,100)
    val seqId1 = diffs(0).seqId

    val up1 = env.diffClient.eventDetail(sessionId, seqId1, ParticipantType.UPSTREAM)
    val down1 = env.diffClient.eventDetail(sessionId, seqId1, ParticipantType.DOWNSTREAM)

    assertEquals(up, up1)
    assertEquals(NO_CONTENT, down1)

    env.addAndNotifyDownstream("abc", env.bizDate(yesterday), down)
    Thread.sleep(2000)
    val diffs2 = tryAgain(sessionId,20,100)
    assertEquals(1, diffs2.length)
    val seqId2 = diffs2(0).seqId

    assertTrue("Invalid sequence ids: seqId1 = %s, seqId2 = %s".format(seqId1, seqId2), seqId2 > seqId1)

    val up2 = env.diffClient.eventDetail(sessionId, seqId2, ParticipantType.UPSTREAM)
    val down2 = env.diffClient.eventDetail(sessionId, seqId2, ParticipantType.DOWNSTREAM)
    assertEquals(up, up2)
    assertEquals(down, down2)
  }

  @Test
  def shouldNotFindDifferencesInParticipantsWithSameStateThatAgentWasntInformedOf {
    env.upstream.addEntity("abc", env.bizDate(yesterday), yesterday, "abcdef")
    env.downstream.addEntity("abc", env.bizDate(yesterday), yesterday, "abcdef")

    val diffs = getReport(env.pairKey, yearAgo, today)

    assertNotNull(diffs)
    assertTrue(diffs.isEmpty)
  }

  @Test
  def shouldReportMatchOnlyAsChangesAreReportedWithinMatchingWindow {

    env.addAndNotifyUpstream("abc", env.bizDate(today), "abcdef")
    env.addAndNotifyDownstream("abc", env.bizDate(today), "abcdef")

    val diffs = getReport(env.pairKey, yearAgo, today)

    assertNotNull(diffs)
    assertTrue(diffs.isEmpty)
        
  }

  def getVerifiedDiffsWithSessionId() = {
    env.upstream.addEntity("abc", env.bizDate(yesterday), yesterday, "abcdef")

    var sessionId = env.diffClient.subscribe(SessionScope.forPairs(env.pairKey), yearAgo, today)
    env.diffClient.runSync(sessionId)
    Thread.sleep(1000)

    val diffs = tryAgain(sessionId,10,100)

    assertNotNull(diffs)
    assertFalse(diffs.isEmpty)
    (diffs, sessionId)
  }

  def tryAgain(sessionId:String, n:Int, wait:Int) = {
    var i = n
    var diffs = env.diffClient.poll(sessionId)
    while(diffs.isEmpty && i > 0) {
      Thread.sleep(wait)

      diffs = env.diffClient.poll(sessionId)
      i-=1
    }
    assertNotNull(diffs)
    diffs
  }

  def testForLink(f:File) = {
      val s = Session.getInstance(new Properties())
      val msg = new MimeMessage(s,new FileInputStream(f))
      val content = msg.getContent.asInstanceOf[String]

      log.debug("Inspecting content from file: " + f.getAbsolutePath)
      log.debug(content)

      val pattern = new Regex(""".*<a href=\"(.*)\".*""", "link");
      pattern.findFirstMatchIn(content) match {
        case None         => fail("Error extracting link from file: " + f.getAbsolutePath)
        case Some(result) => {
          // Attempt to parse the URL, if it is bogus, let the exception fly
          val uri = new URI(result.group("link"))
        }
      }
    }

}