package net.lshift.diffa.kernel.diag

import org.joda.time.DateTime
import collection.mutable.{ListBuffer, HashMap}

/**
 * Local in-memory implementation of the DiagnosticsManager.
 *
 *   TODO: Release resources when pair is removed
 */
class LocalDiagnosticsManager extends DiagnosticsManager {
  private val pairs = HashMap[String, PairDiagnostics]()
  private val maxEventsPerPair = 100

  def logPairEvent(level: DiagnosticLevel, pair: String, msg: String) {
    val pairDiag = pairs.synchronized { pairs.getOrElseUpdate(pair, new PairDiagnostics) }
    pairDiag.logPairEvent(PairEvent(new DateTime(), level, msg))
  }

  def queryEvents(pair:String) = {
    pairs.synchronized { pairs.get(pair) } match {
      case None           => Seq()
      case Some(pairDiag) => pairDiag.queryEvents
    }
  }

  private class PairDiagnostics {
    private val log = ListBuffer[PairEvent]()

    def logPairEvent(evt:PairEvent) {
      log.synchronized {
        log += evt

        val drop = log.length - maxEventsPerPair
        if (drop > 0)
          log.remove(0, drop)
      }
    }

    def queryEvents:Seq[PairEvent] = {
      log.synchronized { log.toSeq }
    }
  }
}