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
import net.lshift.diffa.kernel.events.PairChangeEvent
import akka.actor.{Actor, Scheduler}
import scala.collection.mutable.ListBuffer
import net.jcip.annotations.ThreadSafe
import net.lshift.diffa.kernel.participants.{DownstreamParticipant, UpstreamParticipant}
import java.util.concurrent.ScheduledFuture
import net.lshift.diffa.kernel.differencing._
import java.net.ConnectException

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

  lazy val writer = store.openWriter()

  override def preStart = {
    // schedule a recurring message to flush the writer
    scheduledFlushes = Scheduler.schedule(self, FlushWriterMessage, 0, changeEventQuietTimeoutMillis, MILLISECONDS)
  }

  override def postStop = {
    scheduledFlushes.cancel(true)
  }

  def receive = {
    case ChangeMessage(event)        => {
      policy.onChange(writer, event)

      // if no events have arrived within the timeout period, flush and clear the buffer
      if (lastEventTime < (System.currentTimeMillis() - changeEventBusyTimeoutMillis)) {
        writer.flush()
      }
      lastEventTime = System.currentTimeMillis()
    }
    case DifferenceMessage(diffListener) => {
      try {
        writer.flush()
        policy.difference(pairKey, diffListener)
      } catch {
        case ex => {
          logger.error("Failed to difference pair " + pairKey, ex)
        }
      }
    }
    case SyncAndDifferenceMessage(diffListener, pairSyncListener) => {
      pairSyncListener.pairSyncStateChanged(pairKey, PairSyncState.SYNCHRONIZING)

      val state = try {
        writer.flush()
        policy.syncAndDifference(pairKey, writer, us, ds, diffListener)
        PairSyncState.UP_TO_DATE
      } catch {
        case c:ConnectException => {
          logger.error("Investigate: Participant was not available to synchronize pair: " + pairKey)
          PairSyncState.FAILED
        }
        case x:Exception => {
          logger.error("FAILED to synchronise pair " + pairKey, x)
          PairSyncState.FAILED
        }
      }

      pairSyncListener.pairSyncStateChanged(pairKey, state)

    }

    case FlushWriterMessage         => {
      writer.flush()
    }
  }

}

case class ChangeMessage(event:PairChangeEvent)
case class DifferenceMessage(diffListener:DifferencingListener)
case class SyncAndDifferenceMessage(diffListener:DifferencingListener, pairSyncListener:PairSyncListener)
case object FlushWriterMessage

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
   * Synchronises the participants belonging to the given pair, then generates a different report.
   * Activities are performed on the underlying policy in a thread safe manner, allowing multiple
   * concurrent operations to be submitted safely against the same pair concurrently.
   */
  def syncPair(pairKey:String, diffListener:DifferencingListener, pairSyncListener:PairSyncListener) : Unit
}
