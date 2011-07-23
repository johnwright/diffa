package net.lshift.diffa.kernel.diag

import org.junit.Test
import org.junit.Assert._
import org.hamcrest.CoreMatchers._
import org.joda.time.DateTime
import net.lshift.diffa.kernel.util.HamcrestDateTimeHelpers._
import net.lshift.diffa.kernel.config.{Pair => DiffaPair}

class LocalDiagnosticsManagerTest {
  val diagnostics = new LocalDiagnosticsManager
  val pair = DiffaPair(key="P1", domain="domain")

  @Test
  def shouldAcceptAndStoreLogEventForPair() {
    diagnostics.logPairEvent(DiagnosticLevel.INFO, pair, "Some msg")

    val events = diagnostics.queryEvents(pair, 100)
    assertEquals(1, events.length)
    assertThat(events(0).timestamp,
      is(allOf(after((new DateTime).minusSeconds(5)), before((new DateTime).plusSeconds(1)))))
    assertEquals(DiagnosticLevel.INFO, events(0).level)
    assertEquals("Some msg", events(0).msg)
  }

  @Test
  def shouldLimitNumberOfStoredLogEvents() {
    for (i <- 1 until 1000)
      diagnostics.logPairEvent(DiagnosticLevel.INFO, pair, "Some msg")

    assertEquals(100, diagnostics.queryEvents(pair, 1000).length)
  }
}