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
import collection.mutable.{ListBuffer, HashSet, HashMap}
import java.util.concurrent.atomic.AtomicInteger
import org.joda.time.DateTime

/**
 * Local implementation of the session cache trait. In no way clustered or crash tolerant.
 *
 * TODO: Expire matched events and overridden unmatched events.
 */
class LocalSessionCache(val sessionId:String, private val scope:SessionScope) extends SessionCache {
  private val pending = new HashMap[VersionID, SessionEvent]
  private val events = new ListBuffer[SessionEvent]
  private val seqGenerator = new AtomicInteger(1)

  def currentVersion = events.length match {
    case 0 => "0"
    case _ => events.last.seqId
  }

  def isInScope(id: VersionID) = scope.includes(id.pairKey)

  def addPendingUnmatchedEvent(id:VersionID, lastUpdate:DateTime, upstreamVsn:String, downstreamVsn:String) {
    pending(id) = SessionEvent(null, id, lastUpdate, MatchState.UNMATCHED, upstreamVsn, downstreamVsn)
  }

  def addReportableUnmatchedEvent(id:VersionID, lastUpdate:DateTime, upstreamVsn:String, downstreamVsn:String) = {
    val event = SessionEvent(nextSequenceId.toString, id, lastUpdate, MatchState.UNMATCHED, upstreamVsn, downstreamVsn)

    // Remove any existing unmatched events for the same id
    events.findIndexOf(e => (e.objId == id && e.state == MatchState.UNMATCHED)) match {
      case -1   =>
      case idx  => events.remove(idx)
    }

    events += event
    event
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
    events.findIndexOf(e => { e.objId == id && e.state == MatchState.UNMATCHED } ) match {
      case -1  => null // No un-matched event. Ignore
      case idx => {
        // The unmatched event is now no longer relevant
        events.remove(idx)

        val event = new SessionEvent(nextSequenceId.toString, id, new DateTime(), MatchState.MATCHED, vsn, vsn)
        events += event
        event
      }

    }
  }

  def retrieveAllUnmatchedEvents:Seq[SessionEvent] =
    events.filter(_.state == MatchState.UNMATCHED)

  def retrieveEventsSince(evtSeqId:String):Seq[SessionEvent] = {
    val seqIdNum = Integer.parseInt(evtSeqId)

    events.dropWhile(e => {
      val curSeqIdNum = Integer.parseInt(e.seqId)
      curSeqIdNum <= seqIdNum
    })
  }

  def getEvent(evtSeqId:String) : SessionEvent = {
    // TODO Avoid this linear scan to access an event by its key
    events.takeWhile(e => e.seqId.equals(evtSeqId))(0)  
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