package net.lshift.diffa.kernel.lifecycle

import collection.mutable.ListBuffer
import net.lshift.diffa.kernel.events.VersionID
import org.joda.time.DateTime
import net.lshift.diffa.kernel.differencing.{PairScanState, MatchOrigin, PairSyncListener, DifferencingListener}

/**
 * Central system component for subscribing to notifications. To prevent dependency loops, consumer components should not
 * directly depend on the NotificationCentre, but instead implement AgentLifecycleAware and listen for the
 * <code>onAgentInstantiationCompleted</code> event, which will contain a reference to the center.
 */
class NotificationCentre
    extends DifferencingListener
    with PairSyncListener {
  private val differenceListeners = new ListBuffer[DifferencingListener]
  private val pairSyncListeners = new ListBuffer[PairSyncListener]

  /**
   * Registers a listener to receive different events.
   */
  def registerForDifferenceEvents(l:DifferencingListener) {
    differenceListeners += l
  }

   /**
   * Registers a listener to receive pair sync events.
   */
  def registerForPairSyncEvents(l:PairSyncListener) {
    pairSyncListeners += l
  }

  //
  // Differencing Listener Multicast
  //

  def onMismatch(id: VersionID, lastUpdated: DateTime, upstreamVsn: String, downstreamVsn: String, origin: MatchOrigin) {
    differenceListeners.foreach(_.onMismatch(id, lastUpdated, upstreamVsn, downstreamVsn, origin))
  }
  def onMatch(id: VersionID, vsn: String, origin: MatchOrigin) {
    differenceListeners.foreach(_.onMatch(id, vsn, origin))
  }

  //
  // Pair Sync Listener Multicast
  //

  def pairSyncStateChanged(pairKey: String, syncState: PairScanState) {
    pairSyncListeners.foreach(_.pairSyncStateChanged(pairKey, syncState))
  }
}