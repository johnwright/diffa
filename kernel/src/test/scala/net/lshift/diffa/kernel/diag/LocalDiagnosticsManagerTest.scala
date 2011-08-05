package net.lshift.diffa.kernel.diag

import org.junit.Test
import org.junit.Assert._
import org.hamcrest.CoreMatchers._
import org.joda.time.DateTime
import org.easymock.EasyMock._
import net.lshift.diffa.kernel.util.HamcrestDateTimeHelpers._
import net.lshift.diffa.kernel.differencing.{PairScanState, SessionScope}
import net.lshift.diffa.kernel.frontend.FrontendConversions
import net.lshift.diffa.kernel.config.Endpoint._
import net.lshift.diffa.kernel.config.{Endpoint, DomainConfigStore, Domain, Pair => DiffaPair}

class LocalDiagnosticsManagerTest {
  val domainConfigStore = createStrictMock(classOf[DomainConfigStore])

  val diagnostics = new LocalDiagnosticsManager(domainConfigStore)

  val domainName = "domain"
  val domain = Domain(name=domainName)

  val u = Endpoint(name = "1", scanUrl = "http://foo.com/scan", contentType = "application/json", inboundUrl = "changes", inboundContentType = "application/json")
  val d = Endpoint(name = "2", scanUrl = "http://bar.com/scan", contentType = "application/json", inboundUrl = "changes", inboundContentType = "application/json")

  val pair1 = DiffaPair(key = "pair1", domain = domain, versionPolicyName = "policy", upstream = u, downstream = d)
  val pair2 = DiffaPair(key = "pair2", domain = domain, versionPolicyName = "policy", upstream = u, downstream = d)

  @Test
  def shouldAcceptAndStoreLogEventForPair() {
    diagnostics.logPairEvent(DiagnosticLevel.INFO, pair1, "Some msg")

    val events = diagnostics.queryEvents(pair1, 100)
    assertEquals(1, events.length)
    assertThat(events(0).timestamp,
      is(allOf(after((new DateTime).minusSeconds(5)), before((new DateTime).plusSeconds(1)))))
    assertEquals(DiagnosticLevel.INFO, events(0).level)
    assertEquals("Some msg", events(0).msg)
  }

  @Test
  def shouldLimitNumberOfStoredLogEvents() {
    for (i <- 1 until 1000)
      diagnostics.logPairEvent(DiagnosticLevel.INFO, pair1, "Some msg")

    assertEquals(100, diagnostics.queryEvents(pair1, 1000).length)
  }

  @Test
  def shouldTrackStateOfPairsWithinDomain {
    expect(domainConfigStore.listPairs(domainName)).
        andStubReturn(Seq(FrontendConversions.toPairDef(pair1), FrontendConversions.toPairDef(pair2)))
    replayAll()

    // Query for the states associated. We should get back an entry for pair in "unknown"
    assertEquals(Map("pair1" -> PairScanState.UNKNOWN, "pair2" -> PairScanState.UNKNOWN),
      diagnostics.retrievePairScanStatesForDomain("domain"))

    // Notify that the pair1 is now in Up To Date state
    diagnostics.pairScanStateChanged(pair1, PairScanState.UP_TO_DATE)
    assertEquals(Map("pair1" -> PairScanState.UP_TO_DATE, "pair2" -> PairScanState.UNKNOWN),
      diagnostics.retrievePairScanStatesForDomain("domain"))

    // Notify that the pair2 is now in Failed state
    diagnostics.pairScanStateChanged(pair2, PairScanState.FAILED)
    assertEquals(Map("pair1" -> PairScanState.UP_TO_DATE, "pair2" -> PairScanState.FAILED),
      diagnostics.retrievePairScanStatesForDomain("domain"))

    assertEquals(Map("pair1" -> PairScanState.UP_TO_DATE, "pair2" -> PairScanState.FAILED),
      diagnostics.retrievePairScanStatesForDomain("domain"))

    // Report a scan as being started. We should enter the scanning state again
    diagnostics.pairScanStateChanged(pair1, PairScanState.SCANNING)    // Simulate the supervisor indicating a scan start
    diagnostics.pairScanStateChanged(pair2, PairScanState.SCANNING)    // Simulate the supervisor indicating a scan start
    assertEquals(Map("pair1" -> PairScanState.SCANNING, "pair2" -> PairScanState.SCANNING),
      diagnostics.retrievePairScanStatesForDomain("domain"))

    assertEquals(Map("pair1" -> PairScanState.SCANNING, "pair2" -> PairScanState.SCANNING),
      diagnostics.retrievePairScanStatesForDomain("domain"))
  }

  @Test
  def shouldNotReportStateOfDeletedPairs {
    // Wire up pair1 and pair2 to exist, and provide a status
    expect(domainConfigStore.listPairs(domainName)).
        andStubReturn(Seq(FrontendConversions.toPairDef(pair1), FrontendConversions.toPairDef(pair2)))
    replayAll()

    diagnostics.pairScanStateChanged(pair1, PairScanState.SCANNING)
    assertEquals(Map("pair1" -> PairScanState.SCANNING, "pair2" -> PairScanState.UNKNOWN), diagnostics.retrievePairScanStatesForDomain("domain"))

    // Remove the pair, and report it
    reset(domainConfigStore)
    expect(domainConfigStore.listPairs(domainName)).
        andStubReturn(Seq(FrontendConversions.toPairDef(pair2)))
    replayAll()
    diagnostics.onDeletePair(pair1)
    assertEquals(Map("pair2" -> PairScanState.UNKNOWN), diagnostics.retrievePairScanStatesForDomain(domainName))
  }

  def replayAll() { replay(domainConfigStore) }
}