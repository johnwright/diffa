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
import collection.mutable.Queue
import net.lshift.diffa.kernel.events.{VersionID, PairChangeEvent}
import net.lshift.diffa.kernel.participants.{DownstreamParticipant, UpstreamParticipant}
import org.joda.time.DateTime
import net.lshift.diffa.kernel.util.AlertCodes
import com.eaio.uuid.UUID
import akka.actor._

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
  case class VersionCorrelationWriterCommand(id:UUID, invokeWriter:(LimitedVersionCorrelationWriter => Correlation))
      extends TraceableCommand(id) {
    override def toString = id.toString
  }

  /**
   * This proxy is presented to clients that need access to a LimitedVersionCorrelationWriter.
   * It wraps the underlying writer instance and forwards all commands via asynchronous messages,
   * thus allowing parallel access to the writer.
   */
  private val writerProxy = new LimitedVersionCorrelationWriter() {
    // The receive timeout in seconds
    val timeout = 60

    def clearUpstreamVersion(id: VersionID) = call( _.clearUpstreamVersion(id) )
    def clearDownstreamVersion(id: VersionID) = call( _.clearUpstreamVersion(id) )
    def storeDownstreamVersion(id: VersionID, attributes: Map[String, TypedAttribute], lastUpdated: DateTime, uvsn: String, dvsn: String)
      = call( _.storeDownstreamVersion(id, attributes, lastUpdated, uvsn, dvsn) )
    def storeUpstreamVersion(id: VersionID, attributes: Map[String, TypedAttribute], lastUpdated: DateTime, vsn: String)
      = call( _.storeUpstreamVersion(id, attributes, lastUpdated, vsn) )
    def call(command:(LimitedVersionCorrelationWriter => Correlation)) = {
      val message = VersionCorrelationWriterCommand(lastUUID, command)
      (self !!(message, 1000L * timeout)) match {
        case Some(result) => result.asInstanceOf[Correlation]
        case None         =>
          logger.error("%s: Writer proxy timed out after %s seconds processing command: %s "
                       .format(AlertCodes.RECEIVE_TIMEOUT, timeout, message), new Exception().fillInStackTrace())
          throw new RuntimeException("Writer proxy timeout")
      }
    }
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
            leaveScanState(PairSyncState.FAILED)
          }
          case x                                 =>
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

      // Notify all interested parties
      writer.flush()
      policy.difference(pairKey, currentDiffListener)

      // Re-queue all buffered commands
      leaveScanState(PairSyncState.UP_TO_DATE)
    }
  }

  /**
   * Ensures that the scan state is left cleanly
   */
  def leaveScanState(state:PairSyncState) = {
    // Re-queue all buffered commands
    processBacklog(state)

    // Leave the scan state
    unbecome()
  }

  /**
   * Resets the state of the actor and processes any pending messages that may have arrived during a scan phase.
   */
  def processBacklog(state:PairSyncState) = {
    if (currentScanListener != null) {
      currentScanListener.pairSyncStateChanged(pairKey, state)
    }
    currentDiffListener = null
    currentScanListener = null
    deferred.dequeueAll(d => {self ! d; true})
  }

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

    message.pairSyncListener.pairSyncStateChanged(pairKey, PairSyncState.SYNCHRONIZING)

    try {
      writer.flush()

      Actor.spawn {
        try {
          policy.scanUpstream(pairKey, writerProxy, us, currentDiffListener)
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
          policy.scanDownstream(pairKey, writerProxy, us, ds, currentDiffListener)
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
        processBacklog(PairSyncState.FAILED)
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
}
