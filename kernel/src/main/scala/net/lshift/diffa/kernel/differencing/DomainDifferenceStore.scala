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
import org.joda.time.{Interval, DateTime}
import net.lshift.diffa.kernel.config.DiffaPairRef

/**
 * The domain cache provides facilities for storing difference events that occur, and managing the states of these
 * events. A domain cache instance should exist for each domain that has been created in the system.
 */
trait DomainDifferenceStore {
  /**
   * Indicates that the given domain has been removed, and that any differences stored against it should be removed.
   */
  def removeDomain(domain: String)

  /**
   * Indicates that the given pair has been removed, and that any differences stored against it should be removed.
   */
  def removePair(pair: DiffaPairRef)

  /**
   * Retrieves the current sequence id of the cache
   */
  def currentSequenceId(domain:String):String

  /**
   * Adds a pending event for the given version id into the cache.
   */
  def addPendingUnmatchedEvent(id:VersionID, lastUpdate:DateTime, upstreamVsn:String, downstreamVsn:String, seen:DateTime)

  /**
   * Adds a reportable unmatched event for the given version id into the cache. Returns the detail of the event
   * (including a sequence id). Any previous matched event for the same id will be removed.
   */
  def addReportableUnmatchedEvent(id:VersionID, lastUpdate:DateTime, upstreamVsn:String, downstreamVsn:String, seen:DateTime):DifferenceEvent

  /**
   * Upgrades the given pending event to a reportable event. Returns the detail of the event (including a sequence id).
   * Any previous matched event for the same id will be removed. If no event is available to upgrade with the given
   * id, then null will be returned.
   */
  def upgradePendingUnmatchedEvent(id:VersionID):DifferenceEvent

  /**
   * Indicates that a given pending event is no longer valid - we've received a match, so we won't be upgrading it.
   */
  def cancelPendingUnmatchedEvent(id:VersionID, vsn:String):Boolean

  /**
   * Adds a matched event to the cache. This will result in the removal of any earlier unmatched event for the same id.
   * The matched event will also be marked for expiry at some interval defined by the cache implementation, ensuring
   * that matched events do not result in the cache becoming full.
   */
  def addMatchedEvent(id:VersionID, vsn:String):DifferenceEvent

  /**
   * Indicates that all differences in the cache older than the given date should be marked as matched. This is generally
   * invoked upon a scan being completed, and allows for events that have disappeared to be removed.
   */
  def matchEventsOlderThan(pair:DiffaPairRef, cutoff: DateTime)

  /**
   * Retrieves all unmatched events in the domain that have been added to the cache where their detection timestamp
   * falls within the specified period
   */
  def retrieveUnmatchedEvents(domain:String, interval:Interval) : Seq[DifferenceEvent]

  /**
   * Retrieves all unmatched events that have been added to the cache that have a detection time within the specified
   * interval. The result return a range of the underlying data set that corresponds to the offset and length
   * supplied.
   */
  def retrievePagedEvents(pair: DiffaPairRef, interval:Interval, offset:Int, length:Int) : Seq[DifferenceEvent]

  /**
   * Count the number of events for the given pair within the given interval.
   */
  def countEvents(pair: DiffaPairRef, interval:Interval) : Int

  // TODO document
  def oldestUnmatchedEvent(pair: DiffaPairRef, timespan:Interval) : Option[DifferenceEvent]
  def nextChronologicalUnmatchedEvent(pair: DiffaPairRef, evtSeqId: Int, timespan:Interval) : Option[DifferenceEvent]

  /**
   * Retrieves all events that have occurred within a domain since the provided sequence id.
   * @param evtSeqId the last known sequence id. All events occurring after (not including) this event will be returned.
   * @throws SequenceOutOfDateException if the provided sequence id is too old, and necessary scan information cannot be
   *    provided. A client will need to recover by calling retrieveAllEvents and re-process all events.
   */
  def retrieveEventsSince(domain:String, evtSeqId:String):Seq[DifferenceEvent]

  /**
   * Retrieves a single event by its id.
   * @param evtSeqId sequence id of the event to be retrieved.
   * @throws InvalidSequenceNumberException if the requested sequence id does not exist or has expired.
   */
  def getEvent(domain:String, evtSeqId:String) : DifferenceEvent

  // TODO document
  def retrieveTiledEvents(domain:String, zoomLevel:Int, timespan:Interval) : Map[String,TileSet]
  def retrieveTiledEvents(pair:DiffaPairRef, zoomLevel:Int, timespan:Interval) : TileSet

  /**
   * Indicates that matches older than the given cutoff (based on their seen timestamp) should be removed.
   */
  def expireMatches(cutoff:DateTime)

}