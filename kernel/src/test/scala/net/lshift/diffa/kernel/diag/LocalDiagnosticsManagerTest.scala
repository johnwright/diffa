package net.lshift.diffa.kernel.diag

import org.junit.Assert._
import org.hamcrest.CoreMatchers._
import org.joda.time.DateTime
import org.easymock.EasyMock._
import net.lshift.diffa.kernel.util.HamcrestDateTimeHelpers._
import net.lshift.diffa.kernel.differencing.{PairScanState}
import org.junit.Test
import java.io.OutputStream
import net.lshift.diffa.kernel.config._
import net.lshift.diffa.schema.servicelimits._
import system.SystemConfigStore
import net.lshift.diffa.kernel.frontend.DomainPairDef

class LocalDiagnosticsManagerTest {
  val domainConfigStore = createStrictMock(classOf[DomainConfigStore])
  val systemConfigStore = createStrictMock(classOf[SystemConfigStore])
  val serviceLimitsStore = createStrictMock(classOf[ServiceLimitsStore])
  val explainLogStore = createStrictMock(classOf[ExplainLogStore])

  val diagnostics = new LocalDiagnosticsManager(systemConfigStore, domainConfigStore, serviceLimitsStore, explainLogStore)

  val domainName = "domain"
  val testDomain = Domain(name=domainName)

  val u = Endpoint(name = "1", scanUrl = "http://foo.com/scan", inboundUrl = "changes")
  val d = Endpoint(name = "2", scanUrl = "http://bar.com/scan", inboundUrl = "changes")

  val pair1 = DomainPairDef(key = "pair1", domain = domainName, versionPolicyName = "policy", upstreamName = u.name, downstreamName = d.name)
  val pair2 = DomainPairDef(key = "pair2", domain = domainName, versionPolicyName = "policy", upstreamName = u.name, downstreamName = d.name)

  @Test
  def shouldAcceptAndStoreLogEventForPair() {
    val pairKey = "moderateLoggingPair"
    val pair = DiffaPairRef(pairKey, domainName)

    expectEventBufferLimitQuery(domainName, pairKey, 10)

    diagnostics.logPairEvent(None, pair, DiagnosticLevel.INFO, "Some msg")

    val events = diagnostics.queryEvents(pair, 100)
    assertEquals(1, events.length)
    assertThat(events(0).timestamp,
      is(allOf(after((new DateTime).minusSeconds(5)), before((new DateTime).plusSeconds(1)))))
    assertEquals(DiagnosticLevel.INFO, events(0).level)
    assertEquals("Some msg", events(0).msg)
  }

  @Test
  def shouldLimitNumberOfStoredLogEventsToMax() {

    val pairKey = "maxLoggingPair"

    expectEventBufferLimitQuery(domainName, pairKey, 100)

    val pair = DiffaPairRef(pairKey, domainName)

    for (i <- 1 until 1000)
      diagnostics.logPairEvent(None, pair, DiagnosticLevel.INFO, "Some msg")

    assertEquals(100, diagnostics.queryEvents(pair, 1000).length)
  }

  @Test
  def shouldTrackStateOfPairsWithinDomain() {
    expectPairListFromConfigStore(pair1 :: pair2 :: Nil)

    // Query for the states associated. We should get back an entry for pair in "unknown"
    assertEquals(Map("pair1" -> PairScanState.UNKNOWN, "pair2" -> PairScanState.UNKNOWN),
      diagnostics.retrievePairScanStatesForDomain("domain"))

    // Notify that the pair1 is now in Up To Date state
    diagnostics.pairScanStateChanged(pair1.asRef, PairScanState.UP_TO_DATE)
    assertEquals(Map("pair1" -> PairScanState.UP_TO_DATE, "pair2" -> PairScanState.UNKNOWN),
      diagnostics.retrievePairScanStatesForDomain("domain"))

    // Notify that the pair2 is now in Failed state
    diagnostics.pairScanStateChanged(pair2.asRef, PairScanState.FAILED)
    assertEquals(Map("pair1" -> PairScanState.UP_TO_DATE, "pair2" -> PairScanState.FAILED),
      diagnostics.retrievePairScanStatesForDomain("domain"))

    // Report a scan as being started. We should enter the scanning state again
    diagnostics.pairScanStateChanged(pair1.asRef, PairScanState.SCANNING)    // Simulate the supervisor indicating a scan start
    diagnostics.pairScanStateChanged(pair2.asRef, PairScanState.SCANNING)    // Simulate the supervisor indicating a scan start
    assertEquals(Map("pair1" -> PairScanState.SCANNING, "pair2" -> PairScanState.SCANNING),
      diagnostics.retrievePairScanStatesForDomain("domain"))

  }

  @Test
  def shouldNotReportStateOfDeletedPairs() {
    // Wire up pair1 and pair2 to exist, and provide a status
    expectPairListFromConfigStore(pair1 :: pair2 :: Nil)

    diagnostics.pairScanStateChanged(pair1.asRef, PairScanState.SCANNING)
    assertEquals(Map("pair1" -> PairScanState.SCANNING, "pair2" -> PairScanState.UNKNOWN), diagnostics.retrievePairScanStatesForDomain("domain"))

    // Remove the pair, and report it
    reset(domainConfigStore)
    expect(domainConfigStore.listPairs(domainName)).
        andStubReturn(Seq(pair2))
    replayDomainConfig
    diagnostics.onDeletePair(pair1.asRef)
    assertEquals(Map("pair2" -> PairScanState.UNKNOWN), diagnostics.retrievePairScanStatesForDomain(domainName))
  }

  @Test
  def shouldDelegateToExplainLogStoreWhenExplanationsAreLogged() {
    val pairKey = "explained"
    val pair = DiffaPairRef(pairKey, domainName)

    expect(explainLogStore.logPairExplanation(None, pair, "Test Case", "Diffa did something"))
    replay(explainLogStore)

    diagnostics.logPairExplanation(None, pair, "Test Case", "Diffa did something")

    verify(explainLogStore)
  }

  @Test
  def shouldDelegateToExplainLogStoreWhenExplanationObjectsAreLogged() {
    val pairKey = "explainedobj"
    val pair = DiffaPairRef(pairKey, domainName)

    val timestamp = new DateTime(2012, 8, 3, 11, 53, 07, 123)
    val callback = (os: OutputStream) => os.write("{a: 1}".getBytes("UTF-8"))

    expect(explainLogStore.logPairExplanationAttachment(None, pair, "Test Case", "upstream-foo", timestamp, callback))
    replay(explainLogStore)

    diagnostics.logPairExplanationAttachment(None, pair, "Test Case", "upstream-foo", timestamp, callback)

    verify(explainLogStore)
  }

  private def expectEventBufferLimitQuery(domain:String, pairKey:String, eventBufferSize:Int) = {

    expect(serviceLimitsStore.
      getEffectiveLimitByNameForPair(domain, pairKey, DiagnosticEventBufferSize)).
      andReturn(eventBufferSize).atLeastOnce()

    replay(serviceLimitsStore)
  }

  private def expectPairListFromConfigStore(pairs: Seq[DomainPairDef]) {
    expect(domainConfigStore.listPairs(domainName)).
      andStubReturn(pairs)

    pairs foreach { pairDef =>
      expect(domainConfigStore.getPairDef(domainName, pairDef.key)).
        andStubReturn(pairDef)
    }
    replayDomainConfig
  }

  def replayDomainConfig {
    replay(domainConfigStore)
  }
  
  def replaySystemConfig {
    replay(systemConfigStore)
  }
}
