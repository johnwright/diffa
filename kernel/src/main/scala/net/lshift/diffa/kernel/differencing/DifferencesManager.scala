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
import reflect.BeanProperty
import net.lshift.diffa.kernel.participants.ParticipantType
import org.joda.time.{Interval, DateTime}
import net.lshift.diffa.kernel.config.{DiffaPairRef, Pair => DiffaPair}

/**
 * A DifferencesManager provides a stateful view of the differencing of pairs, and provides mechanisms for polling
 * for changes in the difference states of those pairs.
 */
trait DifferencesManager {
  /**
   * Retrieves a version for the given domain.
   */
  def retrieveDomainVersion(domain:String):String

  /**
   *  Retrieves all events known to this session in the given interval. Will only include unmatched events.
   *  @throws InvalidSessionIDException if the requested session does not exist or has expired.
   */
  def retrieveAllEventsInInterval(domain:String, interval:Interval) : Seq[SessionEvent]

  /**
   *
   * Retrieves all events known to this session. Will only include unmatched events.
   * This pages the results to only contain events that are contained with the specified interval
   * and returns a subset of the underlying data set that corresponds to the offset and length specified.
   * @throws InvalidSessionIDException if the requested session does not exist or has expired.
   */
  def retrievePagedEvents(domain:String, pairKey:String, interval:Interval, offset:Int, length:Int) : Seq[SessionEvent]

  /**
   * Count the number of events for the given pair within the given interval.
   */
  def countEvents(domain:String, pairKey:String, interval:Interval) : Int

  /**
   * Retrieves any additional information that the session manager knows about an event (eg, mismatched hashes,
   * differing content bodies). This information may be retrieved by the manager on demand from remote sources, so
   * should generally only be called on explicit user request.
   */
  def retrieveEventDetail(domain:String, evtSeqId:String, t:ParticipantType.ParticipantType) : String

  /**
   * Informs the difference manager that a pair has been updated.
   */
  def onUpdatePair(pairRef: DiffaPairRef)

  /**
   * Informs the difference manager that a pair has been deleted.
   */
  def onDeletePair(pairRef: DiffaPairRef)

  /**
   * Informs the difference manager that a domain has been updated.
   */
  def onUpdateDomain(domain: String)

  /**
   * Informs the difference manager that a domain has been deleted.
   */
  def onDeleteDomain(domain: String)
}

class InvalidSessionIDException extends Exception
class InvalidSequenceNumberException(val id:String) extends Exception(id)
class SequenceOutOfDateException extends Exception

case class SessionEvent(
  @BeanProperty var seqId:String,
  @BeanProperty var objId:VersionID,
  @BeanProperty var detectedAt:DateTime,
  @BeanProperty var state:MatchState,
  @BeanProperty var upstreamVsn:String,
  @BeanProperty var downstreamVsn:String) {

 def this() = this(null, null, null, MatchState.UNMATCHED, null, null)
    
}

