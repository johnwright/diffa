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
import net.jcip.annotations.ThreadSafe
import java.util.concurrent.ScheduledFuture
import net.lshift.diffa.kernel.differencing._
import net.lshift.diffa.kernel.events.{VersionID, PairChangeEvent}
import net.lshift.diffa.kernel.participants.{DownstreamParticipant, UpstreamParticipant}
import org.joda.time.DateTime
import net.lshift.diffa.kernel.util.AlertCodes
import com.eaio.uuid.UUID
import akka.actor._
import collection.mutable.{SynchronizedQueue, Queue}

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

  private var currentDiffListener:DifferencingListener = null
  private var currentScanListener:PairSyncListener = null
  private var upstreamSuccess = false
  private var downstreamSuccess = false

  /**
   * Thread safe buffer of match events that will be accessed directly by different sub actors
   */
  @ThreadSafe
  private val bufferedMatchEvents = new SynchronizedQueue[MatchEvent]

  lazy val writer = store.openWriter()

  /**
   * A queue of deferred messages that arrived during a scanning state
   */
  private val deferred = new Queue[Deferrable]

  /**
   * This UUID is used to group messages of with in the same scan operation
   */
  var lastUUID = new UUID

  /**
   * This allows tracing of spurious messages, but is only enabled in when the log level is set to TRACE
   */
  abstract class TraceableCommand(uuid:UUID) {
    var exception:Throwable = null
    if (logger.isTraceEnabled) {
      exception = new Exception().fillInStackTrace()
    }
  }

  /**
   * This is the set of commands that the writer proxy understands
   */
  case class VersionCorrelationWriterCommand(_uuid:UUID, invokeWriter:(LimitedVersionCorrelationWriter => Correlation))
      extends TraceableCommand(_uuid)

  /**
   * This proxy is presented to clients that need access to a LimitedVersionCorrelationWriter.
   * It wraps the underlying writer instance and forwards all commands via asynchronous messages,
   * thus allowing parallel access to the writer.
   */
  private val writerProxy = new LimitedVersionCorrelationWriter() {
    def clearUpstreamVersion(id: VersionID) = get(self !! VersionCorrelationWriterCommand(lastUUID, _.clearUpstreamVersion(id) ) )
    def clearDownstreamVersion(id: VersionID) = get(self !! VersionCorrelationWriterCommand(lastUUID, _.clearUpstreamVersion(id) ) )
    def storeDownstreamVersion(id: VersionID, attributes: Map[String, TypedAttribute], lastUpdated: DateTime, uvsn: String, dvsn: String)
      = get(self !! VersionCorrelationWriterCommand(lastUUID, _.storeDownstreamVersion(id, attributes, lastUpdated, uvsn, dvsn) ) )
    def storeUpstreamVersion(id: VersionID, attributes: Map[String, TypedAttribute], lastUpdated: DateTime, vsn: String)
      = get(self !! VersionCorrelationWriterCommand(lastUUID, _.storeUpstreamVersion(id, attributes, lastUpdated, vsn) ) )
    def get(f:Option[Any]) = f.get.asInstanceOf[Correlation]
  }

  case class MatchEvent(id: VersionID, command:(DifferencingListener => Unit))

  private val bufferingListener = new DifferencingListener {

    /**
     * Buffer up the match event because these won't be replayed by a subsequent difference operation.
     */
    def onMatch(id:VersionID, vsn:String) = bufferedMatchEvents.enqueue(MatchEvent(id, _.onMatch(id, vsn)))

    /**
     * Drop the mismatch, since we will be doing a full difference and the end of the scan process.
     */
    def onMismatch(id:VersionID, update:DateTime, uvsn:String, dvsn:String) = ()
  }

  override def preStart = {
    // schedule a recurring message to flush the writer
    scheduledFlushes = Scheduler.schedule(self, FlushWriterMessage, 0, changeEventQuietTimeoutMillis, MILLISECONDS)
  }

  override def postStop = scheduledFlushes.cancel(true)

  /**
   * Main receive loop of this actor. This is effectively a FSM.
   * When the scan state is entered, all non-writerProxy commands are buffered up
   * and will be re-delivered into this actor's mailbox when the scan state is exited.
   */
  def receive = {
    case s:ScanMessage => {
      lastUUID = new UUID
      if (handleScanMessage(s)) {
        become {
          case FlushWriterMessage                => // ignore flushes in this state - we may want to roll the index back
          case CancelMessage                     => {
            logger.info("%s: Scan for pair %s was cancelled on request".format(AlertCodes.CANCELLATION_REQUEST, pairKey))
            flushBufferedEvents
            dropPendingScans
            leaveScanState(PairScanState.CANCELLED)
          }
          case c:VersionCorrelationWriterCommand => self.reply(c.invokeWriter(writer))
          case d:Deferrable                      => deferred.enqueue(d)
          case UpstreamScanSuccess(uuid)         => {
            logger.trace("Received upstream success: %s".format(uuid))
            upstreamSuccess = true
            checkForCompletion
          }
          case DownstreamScanSuccess(uuid)       => {
            logger.trace("Received downstream success: %s".format(uuid))
            downstreamSuccess = true
            checkForCompletion
          }
          case ScanFailed(uuid)                  => {
            logger.error("Received scan failure: %s".format(uuid))
            leaveScanState(PairScanState.FAILED)
          }
          case x =>
            // TODO should the scan state be exited at this stage?
            logger.error("%s: Spurious message: %s".format(AlertCodes.SPURIOUS_ACTOR_MESSAGE, x))
        }
      }
    }
    case c:ChangeMessage                   => handleChangeMessage(c)
    case d:DifferenceMessage               => handleDifferenceMessage(d)
    case FlushWriterMessage                => writer.flush()
    case c:VersionCorrelationWriterCommand =>  {
      logger.error("%s: Received command (%s) in non-scanning state - potential bug"
                  .format(AlertCodes.OUT_OF_ORDER_MESSAGE, c), c.exception)
    }
    case ScanFailed(uuid)               => {
      logger.error("%s: Received scan failure (%s) in non-scanning state - potential bug"
                   .format(AlertCodes.OUT_OF_ORDER_MESSAGE, uuid))
    }
    case x            =>
      logger.error("%s: Spurious message: %s".format(AlertCodes.SPURIOUS_ACTOR_MESSAGE, x))
  }

  /**
   * Exit the scanning state and notify interested parties
   */
  def checkForCompletion = {
    if (upstreamSuccess && downstreamSuccess) {
      downstreamSuccess = false
      upstreamSuccess = false
      logger.trace("Finished scan %s".format(lastUUID))

      // Feed all of the match events out to the interested parties
      flushBufferedEvents

      // Notify all interested parties of all of the outstanding mismatches
      writer.flush()
      policy.difference(pairKey, currentDiffListener)

      // Re-queue all buffered commands
      leaveScanState(PairScanState.UP_TO_DATE)
    }
  }

  def flushBufferedEvents = bufferedMatchEvents.dequeueAll(e => {e.command(currentDiffListener); true})

  /**
   * Ensures that the scan state is left cleanly
   */
  def leaveScanState(state:PairScanState) = {

    if (state == PairScanState.FAILED || state == PairScanState.CANCELLED) {
      writer.rollback()
    }

    // Re-queue all buffered commands
    processBacklog(state)

    // Make sure that the event queue is empty for the next scan
    bufferedMatchEvents.clear()

    // Leave the scan state
    unbecome()
  }

  /**
   * Resets the state of the actor and processes any pending messages that may have arrived during a scan phase.
   */
  def processBacklog(state:PairScanState) = {
    if (currentScanListener != null) {
      currentScanListener.pairSyncStateChanged(pairKey, state)
    }
    currentDiffListener = null
    currentScanListener = null
    deferred.dequeueAll(d => {self ! d; true})
  }

  def dropPendingScans = deferred.dequeueAll(d => d.isInstanceOf[ScanMessage])

  /**
   * Events out normal changes.
   */
  def handleChangeMessage(message:ChangeMessage) = {
    policy.onChange(writer, message.event)
    // if no events have arrived within the timeout period, flush and clear the buffer
    if (lastEventTime < (System.currentTimeMillis() - changeEventBusyTimeoutMillis)) {
      writer.flush()
    }
    lastEventTime = System.currentTimeMillis()
  }

  /**
   * Runs a simple difference for the pair.
   */
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

  /**
   * Implements the top half of the request to scan the participants for digests.
   * This actor will still be in the scan state after this callback has returned.
   */
  def handleScanMessage(message:ScanMessage) : Boolean = {
    // squirrel some callbacks away for invocation in subsequent receives in the scanning state
    currentDiffListener = message.diffListener
    currentScanListener = message.pairSyncListener

    // Make sure that the event buffer is empty for this scan
    if (bufferedMatchEvents.size > 0) {
      logger.warn("Found %s match events in the buffer, possible bug".format(bufferedMatchEvents.size))
      bufferedMatchEvents.clear()
    }

    message.pairSyncListener.pairSyncStateChanged(pairKey, PairScanState.SYNCHRONIZING)

    try {
      writer.flush()

      Actor.spawn {
        try {
          policy.scanUpstream(pairKey, writerProxy, us, bufferingListener)
          self ! UpstreamScanSuccess(lastUUID)
        }
        catch {
          case e:Exception => {
            logger.error("Upstream scan failed: " + pairKey, e)
            self ! ScanFailed(lastUUID)
          }
        }
      }

      Actor.spawn {
        try {
          policy.scanDownstream(pairKey, writerProxy, us, ds, bufferingListener)
          self ! DownstreamScanSuccess(lastUUID)
        }
        catch {
          case e:Exception => {
            logger.error("Downstream scan failed: " + pairKey, e)
            self ! ScanFailed(lastUUID)
          }
        }
      }

      true

    } catch {
      case x: Exception => {
        logger.error("Failed to initiate scan for pair: " + pairKey, x)
        processBacklog(PairScanState.FAILED)
        false
      }
    }
  }
}


/**
 * Marker messages to let the actor know that a portion of the scan has successfully completed.
 */
case class UpstreamScanSuccess(uuid:UUID)
case class DownstreamScanSuccess(uuid:UUID)
case class ScanFailed(uuid:UUID)

/**
 * This is the group of all commands that should be buffered when the actor is the scan state.
 */
abstract class Deferrable
case class ChangeMessage(event: PairChangeEvent) extends Deferrable
case class DifferenceMessage(diffListener: DifferencingListener) extends Deferrable
case class ScanMessage(diffListener: DifferencingListener, pairSyncListener: PairSyncListener) extends Deferrable

/**
 * This message indicates that this actor should cancel all current and pending scan operations.
 */
case object CancelMessage
/**
 * An internal command that indicates to the actor that the underlying writer should be flushed
 */
private case object FlushWriterMessage extends Deferrable

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
   * beforehand - use <code>scanPair</code> to do the sync first.
   */
  def difference(pairKey:String, diffListener:DifferencingListener)

  /**
   * Synchronises the participants belonging to the given pair, then generates a different report.
   * Activities are performed on the underlying policy in a thread safe manner, allowing multiple
   * concurrent operations to be submitted safely against the same pair concurrently.
   */
  def scanPair(pairKey:String, diffListener:DifferencingListener, pairSyncListener:PairSyncListener) : Unit

  /**
   * Cancels any scan operation that may be in process.
   * This is a blocking call, so it will only return after all current and pending scans have been cancelled.
   */
  def cancelAllScans(pairKey:String) : Unit
}
