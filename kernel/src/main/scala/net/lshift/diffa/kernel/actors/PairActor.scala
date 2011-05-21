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

package net.lshift.diffa.kernel.actors

import java.util.concurrent.TimeUnit.MILLISECONDS
import org.slf4j.{Logger, LoggerFactory}
import akka.actor.{Actor, Scheduler}
import net.jcip.annotations.ThreadSafe
import java.util.concurrent.ScheduledFuture
import net.lshift.diffa.kernel.differencing._
import java.net.ConnectException
import collection.mutable.{Queue, ListBuffer}
import net.lshift.diffa.kernel.events.{VersionID, PairChangeEvent}
import net.lshift.diffa.kernel.config.Pair
import net.lshift.diffa.kernel.participants.{Participant, DownstreamParticipant, UpstreamParticipant}
import org.joda.time.DateTime
import akka.dispatch.Future
import util.matching.Regex.Match
import javax.persistence.criteria.CriteriaBuilder.Case

/**
 * This actor serializes access to the underlying version policy from concurrent processes.
 */
case class PairActor(pairKey:String,
                     us:UpstreamParticipant,
                     ds:DownstreamParticipant,
                     policy:VersionPolicy,
                     store:VersionCorrelationStore,
                     changeEventBusyTimeoutMillis: Long,
                     changeEventQuietTimeoutMillis: Long) extends Actor {

  val logger:Logger = LoggerFactory.getLogger(getClass)

  self.id_=(pairKey)

  private var lastEventTime: Long = 0
  private var scheduledFlushes: ScheduledFuture[_] = _

  /**
   * FSM: This indicates whether the actor has entered the scanning state
   */
  private var scanning = false
  private var currentDiffListener:DifferencingListener = null
  private var currentScanListener:PairSyncListener = null
  private var upstreamSuccess = false
  private var downstreamSuccess = false

  /**
   * A queue of deferred messages that arrived during a scanning state
   */
  private val deferred = new Queue[Deferrable]

  abstract case class VersionCorrelationWriterCommand
  case class ClearDownstreamVersion(id: VersionID) extends VersionCorrelationWriterCommand
  case class ClearUpstreamVersion(id: VersionID) extends VersionCorrelationWriterCommand
  case class StoreDownstreamVersion(id: VersionID, attributes: Map[String, TypedAttribute], lastUpdated: DateTime, uvsn: String, dvsn: String) extends VersionCorrelationWriterCommand
  case class StoreUpstreamVersion(id: VersionID, attributes: Map[String, TypedAttribute], lastUpdated: DateTime, vsn: String) extends VersionCorrelationWriterCommand

  private val writerProxy = new VersionCorrelationWriter() {
    def flush() = null
    def isDirty = false
    def clearDownstreamVersion(id: VersionID) = getOrThrow(self !!! ClearDownstreamVersion(id))
    def clearUpstreamVersion(id: VersionID) = getOrThrow(self !!! ClearUpstreamVersion(id))
    def storeDownstreamVersion(id: VersionID, attributes: Map[String, TypedAttribute], lastUpdated: DateTime, uvsn: String, dvsn: String)
      = getOrThrow(self !!! StoreDownstreamVersion(id, attributes, lastUpdated, uvsn, dvsn))
    def storeUpstreamVersion(id: VersionID, attributes: Map[String, TypedAttribute], lastUpdated: DateTime, vsn: String)
      = getOrThrow(self !!! StoreUpstreamVersion(id, attributes, lastUpdated, vsn))

    def getOrThrow[T](f:Future[T]) = f.result.getOrElse(throw new RuntimeException("Writer proxy message timeout"))
  }

  def handleWriterCommand(command:VersionCorrelationWriterCommand) = command match {
    case ClearDownstreamVersion(id) => writer.clearDownstreamVersion(id)
    case ClearUpstreamVersion(id)   => writer.clearUpstreamVersion(id)
    case StoreDownstreamVersion(id, attributes, lastUpdated, uvsn, dvsn) => writer.storeDownstreamVersion(id, attributes, lastUpdated, uvsn, dvsn)
    case StoreUpstreamVersion(id, attributes, lastUpdated, vsn) => writer.storeUpstreamVersion(id, attributes, lastUpdated, vsn)
  }

  lazy val writer = store.openWriter()

  override def preStart = {
    // schedule a recurring message to flush the writer
    scheduledFlushes = Scheduler.schedule(self, FlushWriterMessage, 0, changeEventQuietTimeoutMillis, MILLISECONDS)
  }

  override def postStop = {
    scheduledFlushes.cancel(true)
  }

  def receive = {

    case c:VersionCorrelationWriterCommand if scanning => handleWriterCommand(c)
    case d:Deferrable   if scanning   => deferred.enqueue(d)
    case d:Deferrable   if !scanning  => handleDeferrable(d)

    case UpstreamScanSuccess if scanning => {
      upstreamSuccess = true
      checkForCompletion
    }
    case DownstreamScanSuccess if scanning => {
      downstreamSuccess = true
      checkForCompletion
    }

    case x => logger.error("Spurious message: %s".format(x))
  }

  def initiateScan(participant:Participant) = {
    try {
      participant match {
        case u:UpstreamParticipant    => policy.scanUpstream(pairKey, writerProxy, us, currentDiffListener)
        case d:DownstreamParticipant  => policy.scanDownstream(pairKey, writerProxy, us, ds, currentDiffListener)
      }
    }
    catch {
      case x:Exception => {
        processBacklog(PairSyncState.FAILED)
      }
    }
  }
  /**
   * Exit the scanning state and notify interested parties
   */
  def checkForCompletion = {
    if (upstreamSuccess && downstreamSuccess) {
      processBacklog(PairSyncState.UP_TO_DATE)
    }
  }

  // TODO test this
  def processBacklog(state:PairSyncState) = {
    scanning = false
    currentScanListener.pairSyncStateChanged(pairKey, state)
    currentDiffListener = null
    currentScanListener = null
    // TODO I can't beleive this is how you drain a queue using Scala collections
    deferred.dequeueAll(d => true).foreach(handleDeferrable(_))
  }

  def handleDeferrable(d:Deferrable) : Unit = d match {
    case c:ChangeMessage            => handleChangeMessage(c)
    case d:DifferenceMessage        => handleDifferenceMessage(d)
    case s:ScanAndDifferenceMessage => handleScanAndDifferenceMessage(s)
    case FlushWriterMessage         => handleFlushWriterMessage
  }

  def handleChangeMessage(message:ChangeMessage) = {
    policy.onChange(writer, message.event)
    // if no events have arrived within the timeout period, flush and clear the buffer
    if (lastEventTime < (System.currentTimeMillis() - changeEventBusyTimeoutMillis)) {
      writer.flush()
    }
    lastEventTime = System.currentTimeMillis()
  }

  def handleDifferenceMessage(message:DifferenceMessage) = {
    try {
      writer.flush()
      policy.difference(pairKey, message.diffListener)
    } catch {
      case ex => {
        logger.error("Failed to difference pair " + pairKey, ex)
      }
    }
  }

  def handleScanAndDifferenceMessage(message:ScanAndDifferenceMessage) = {
    scanning = true

    // squirrel some callbacks away for invocation in subsequent receives in the scanning state
    currentDiffListener = message.diffListener
    currentScanListener = message.pairSyncListener

    message.pairSyncListener.pairSyncStateChanged(pairKey, PairSyncState.SYNCHRONIZING)

    var s = self

    try {
      writer.flush()

      Actor.spawn {
        initiateScan(us)
        self ! UpstreamScanSuccess
      }

      Actor.spawn {
        initiateScan(ds)
        self ! DownstreamScanSuccess
      }

    } catch {
      case x: Exception => {
        logger.error("Failed to initiate scan for pair: " + pairKey, x)
        message.pairSyncListener.pairSyncStateChanged(pairKey, PairSyncState.FAILED)
      }
    }
  }

  def handleFlushWriterMessage = writer.flush()

}

case object UpstreamScanSuccess
case object DownstreamScanSuccess

abstract class Deferrable
case class ChangeMessage(event: PairChangeEvent) extends Deferrable
case class DifferenceMessage(diffListener: DifferencingListener) extends Deferrable
case class ScanAndDifferenceMessage(diffListener: DifferencingListener, pairSyncListener: PairSyncListener) extends Deferrable
// TODO Is this ever requested by another component any more?
case object FlushWriterMessage extends Deferrable

/**
 * This is a thread safe entry point to an underlying version policy.
 */
@ThreadSafe
trait PairPolicyClient {

  /**
   * Propagates the change event to the underlying policy implementation in a serial fashion.
   */
  def propagateChangeEvent(event:PairChangeEvent) : Unit

  /**
   * Runs a difference report based on stored data for the given pair. Does not synchronise with the participants
   * beforehand - use <code>syncPair</code> to do the sync first.
   */
  def difference(pairKey:String, diffListener:DifferencingListener)

  /**
   * TODO This is just a scan, not a sync
   * Synchronises the participants belonging to the given pair, then generates a different report.
   * Activities are performed on the underlying policy in a thread safe manner, allowing multiple
   * concurrent operations to be submitted safely against the same pair concurrently.
   */
  def syncPair(pairKey:String, diffListener:DifferencingListener, pairSyncListener:PairSyncListener) : Unit
}
