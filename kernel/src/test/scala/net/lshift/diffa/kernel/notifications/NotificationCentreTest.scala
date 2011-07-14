package net.lshift.diffa.kernel.notifications

import org.junit.Test
import net.lshift.diffa.kernel.lifecycle.NotificationCentre
import org.easymock.EasyMock._
import net.lshift.diffa.kernel.events.VersionID
import org.joda.time.DateTime
import net.lshift.diffa.kernel.differencing.{PairScanState, PairSyncListener, TriggeredByScan, DifferencingListener}

/**
 * Test cases for the Notification Centre.
 */
class NotificationCentreTest {
  val nc = new NotificationCentre

  @Test
  def shouldDispatchToDifferencingListeners() {
    val l1 = createStrictMock("l1", classOf[DifferencingListener])
    val l2 = createStrictMock("l2", classOf[DifferencingListener])
    val now = new DateTime(2011, 07, 14, 14, 49, 0, 0)

    nc.registerForDifferenceEvents(l1)
    nc.registerForDifferenceEvents(l2)

    l1.onMatch(VersionID("p", "e"), "v", TriggeredByScan)
    l2.onMatch(VersionID("p", "e"), "v", TriggeredByScan)
    l1.onMismatch(VersionID("p", "e"), now, "uv", "dv", TriggeredByScan)
    l2.onMismatch(VersionID("p", "e"), now, "uv", "dv", TriggeredByScan)
    replay(l1, l2)

    nc.onMatch(VersionID("p", "e"), "v", TriggeredByScan)
    nc.onMismatch(VersionID("p", "e"), now, "uv", "dv", TriggeredByScan)
    verify(l1, l2)
  }

  @Test
  def shouldDispatchToPairSyncListeners() {
    val l1 = createStrictMock("l1", classOf[PairSyncListener])
    val l2 = createStrictMock("l2", classOf[PairSyncListener])
    
    nc.registerForPairSyncEvents(l1)
    nc.registerForPairSyncEvents(l2)

    l1.pairSyncStateChanged("p", PairScanState.SYNCHRONIZING)
    l2.pairSyncStateChanged("p", PairScanState.SYNCHRONIZING)
    replay(l1, l2)

    nc.pairSyncStateChanged("p", PairScanState.SYNCHRONIZING)
    verify(l1, l2)
  }
}