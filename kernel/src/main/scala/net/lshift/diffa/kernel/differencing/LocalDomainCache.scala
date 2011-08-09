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

import net.lshift.diffa.kernel.events.VersionID
import java.lang.String
import java.util.concurrent.atomic.AtomicInteger
import collection.mutable.{LinkedHashMap, HashMap}
import org.joda.time.{Interval, DateTime}

/**
 * Local implementation of the domain cache trait. In no way clustered or crash tolerant.
 *
 * TODO: Expire matched events and overridden unmatched events.
 */
class LocalDomainCache(val domain:String) extends DomainCache {
  private val pending = new HashMap[VersionID, DifferenceEvent]
  private val seqIdsByVersionId = new HashMap[VersionID, String]
  private val eventsBySeqId = new LinkedHashMap[String,DifferenceEvent]
  private val seqGenerator = new AtomicInteger(1)

  def currentSequenceId = eventsBySeqId.size match {
    case 0 => "0"
    case _ => {
      val (_, event) = eventsBySeqId.last
      event.seqId
    }
  }

  def addPendingUnmatchedEvent(id:VersionID, lastUpdate:DateTime, upstreamVsn:String, downstreamVsn:String) {
    pending(id) = DifferenceEvent(null, id, lastUpdate, MatchState.UNMATCHED, upstreamVsn, downstreamVsn)
  }

  def addReportableUnmatchedEvent(id:VersionID, lastUpdate:DateTime, upstreamVsn:String, downstreamVsn:String) = {
    def report() = nextSequence(id, lastUpdate, upstreamVsn, downstreamVsn, MatchState.UNMATCHED)
    def remove(existingSeqId:String) = { seqIdsByVersionId.remove(id); eventsBySeqId.remove(existingSeqId) }

    seqIdsByVersionId.get(id) match  {
      case Some(existingSeqId) =>
        val existing = eventsBySeqId(existingSeqId)
        existing.state match {
          case MatchState.UNMATCHED =>
            // We've already got an unmatched event. See if it matches all the criteria.
            if (existing.upstreamVsn == upstreamVsn && existing.downstreamVsn == downstreamVsn) {
              existing
            } else {
              remove(existingSeqId)
              report()
            }

          case MatchState.MATCHED =>
              // The difference has re-occurred. Remove the match, and add a difference.
            remove(existingSeqId)
            report()
        }

      case None =>
        report()
    }
  }

  def upgradePendingUnmatchedEvent(id:VersionID) = {
    pending.remove(id) match {
      case Some(DifferenceEvent(_, _, lastUpdate, _, upstreamVsn, downstreamVsn)) =>
        addReportableUnmatchedEvent(id, lastUpdate, upstreamVsn, downstreamVsn)
      case None => null
    }
  }

  def cancelPendingUnmatchedEvent(id:VersionID, vsn:String) = {
    pending.get(id) match {
      case Some(event) =>
        if (event.upstreamVsn == vsn) {
          pending.remove(id).get
        } else {
          null
        }
      case None => null
    }
  }

  def addMatchedEvent(id:VersionID, vsn:String) = {
    // Ensure there is an unmatched event to override
    seqIdsByVersionId.get(id) match {
      case Some(existingSeqId) =>
        val existing = eventsBySeqId(existingSeqId)
        existing.state match {
          case MatchState.MATCHED => // Ignore. We've already got an event for what we want.
            existing
          case MatchState.UNMATCHED =>
            // A difference has gone away. Remove the difference, and add in a match
            seqIdsByVersionId.remove(id)
            eventsBySeqId.remove(existingSeqId)
            nextSequence(id, new DateTime, vsn, vsn, MatchState.MATCHED)
        }
      case None =>
        null
    }
  }

  def nextSequence(id:VersionID, lastUpdate:DateTime, upstreamVsn:String, downstreamVsn:String, state:MatchState) = {
    val sequence = nextSequenceId.toString
    val event = new DifferenceEvent(sequence, id, lastUpdate, state, upstreamVsn, downstreamVsn)
    eventsBySeqId(sequence) = event
    seqIdsByVersionId(id) = sequence
    event
  }

  def retrieveUnmatchedEvents(interval:Interval) =
    eventsBySeqId.filter(p => p._2.state == MatchState.UNMATCHED && interval.contains(p._2.detectedAt)).values.toSeq

  def retrievePagedEvents(pairKey:String, interval:Interval, offset:Int, length:Int) =
    retrieveUnmatchedEvents(interval).filter(_.objId.pair.key == pairKey).slice(offset, offset + length)

  def countEvents(pairKey: String, interval: Interval) =
    retrieveUnmatchedEvents(interval).filter(_.objId.pair.key == pairKey).length

  def retrieveEventsSince(evtSeqId:String):Seq[DifferenceEvent] = {
    val seqIdNum = Integer.parseInt(evtSeqId)

    eventsBySeqId.dropWhile(p => {
      val curSeqIdNum = Integer.parseInt(p._1)
      curSeqIdNum <= seqIdNum
    }).values.toSeq
  }

  def getEvent(evtSeqId:String) : DifferenceEvent = {    
    eventsBySeqId.get(evtSeqId) match {
      case None    => throw new InvalidSequenceNumberException(evtSeqId)
      case Some(e) => e
    }
  }

  private def nextSequenceId = seqGenerator.getAndIncrement
}

class LocalDomainCacheProvider extends DomainCacheProvider {
  private val domains = new HashMap[String, LocalDomainCache]

  def retrieveCache(domain: String) = domains.synchronized { domains.get(domain) }
  def retrieveOrAllocateCache(domain: String) = domains.synchronized {
    domains.get(domain) match {
      case Some(s) => s
      case None => {
        val domainCache = new LocalDomainCache(domain)
        domains(domain) = domainCache
        domainCache
      }
    }
  }
}