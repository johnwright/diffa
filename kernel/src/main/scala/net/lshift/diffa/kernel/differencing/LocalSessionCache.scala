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

import net.lshift.diffa.kernel.events.VersionID
import java.lang.String
import java.util.concurrent.atomic.AtomicInteger
import org.joda.time.DateTime
import collection.mutable.{LinkedHashMap, HashMap}

/**
 * Local implementation of the session cache trait. In no way clustered or crash tolerant.
 *
 * TODO: Expire matched events and overridden unmatched events.
 */
class LocalSessionCache(val sessionId:String, private val scope:SessionScope) extends SessionCache {
  private val pending = new HashMap[VersionID, SessionEvent]
  private val events = new LinkedHashMap[String,SessionEvent]
  private val seqGenerator = new AtomicInteger(1)

  def currentVersion = events.size match {
    case 0 => "0"
    case _ => {
      val (_, session) = events.last
      session.seqId
    }
  }

  def isInScope(id: VersionID) = scope.includes(id.pairKey)

  def addPendingUnmatchedEvent(id:VersionID, lastUpdate:DateTime, upstreamVsn:String, downstreamVsn:String) {
    pending(id) = SessionEvent(null, id, lastUpdate, MatchState.UNMATCHED, upstreamVsn, downstreamVsn)
  }

  def addReportableUnmatchedEvent(id:VersionID, lastUpdate:DateTime, upstreamVsn:String, downstreamVsn:String) = {
    events.find( p => (p._2.objId == id && p._2.state == MatchState.UNMATCHED) ) match {
      case None    =>
      case Some(x) => events -= x._1
    }
    nextSequence(id, lastUpdate, upstreamVsn, downstreamVsn, MatchState.UNMATCHED)
  }

  def upgradePendingUnmatchedEvent(id:VersionID) = {
    pending.remove(id) match {
      case Some(SessionEvent(_, _, lastUpdate, _, upstreamVsn, downstreamVsn)) =>
        addReportableUnmatchedEvent(id, lastUpdate, upstreamVsn, downstreamVsn)
      case None => null
    }
  }

  def addMatchedEvent(id:VersionID, vsn:String) = {
    // Ensure there is an unmatched event to override
    events.find( p => (p._2.objId == id && p._2.state == MatchState.UNMATCHED) ) match {
      case None    => null
      case Some(x) => {
        events -= x._1
        nextSequence(id, new DateTime, vsn, vsn, MatchState.MATCHED)
      }
    }
  }

  def nextSequence(id:VersionID, lastUpdate:DateTime, upstreamVsn:String, downstreamVsn:String, state:MatchState) = {
    val sequence = nextSequenceId.toString
    val event = new SessionEvent(sequence, id, lastUpdate, state, upstreamVsn, downstreamVsn)
    events(sequence)= event
    event
  }

  def retrieveAllUnmatchedEvents:Seq[SessionEvent] =
    events.filter(p => p._2.state == MatchState.UNMATCHED).values.toSeq

  def retrieveEventsSince(evtSeqId:String):Seq[SessionEvent] = {
    val seqIdNum = Integer.parseInt(evtSeqId)

    events.dropWhile(p => {
      val curSeqIdNum = Integer.parseInt(p._1)
      curSeqIdNum <= seqIdNum
    }).values.toSeq
  }

  def getEvent(evtSeqId:String) : SessionEvent = {    
    events.get(evtSeqId) match {
      case None    => throw new InvalidSequenceNumberException(evtSeqId)
      case Some(e) => e
    }
  }

  private def nextSequenceId = seqGenerator.getAndIncrement
}

class LocalSessionCacheProvider extends SessionCacheProvider {
  private val sessions = new HashMap[String, LocalSessionCache]

  def retrieveCache(sessionID: String) = sessions.synchronized { sessions.get(sessionID) }
  def retrieveOrAllocateCache(sessionID: String, scope:SessionScope) = sessions.synchronized {
    sessions.get(sessionID) match {
      case Some(s) => s
      case None => {
        val session = new LocalSessionCache(sessionID, scope)
        sessions(sessionID) = session
        session
      }
    }
  }
}