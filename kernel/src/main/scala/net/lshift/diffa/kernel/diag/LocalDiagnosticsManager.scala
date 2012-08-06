package net.lshift.diffa.kernel.diag

import collection.mutable.{ListBuffer, HashMap}
import net.lshift.diffa.kernel.differencing.{PairScanState, PairScanListener}
import net.lshift.diffa.kernel.lifecycle.{NotificationCentre, AgentLifecycleAware}
import net.lshift.diffa.kernel.config.system.SystemConfigStore
import net.lshift.diffa.kernel.config.{PairServiceLimitsView, DiffaPairRef, DomainConfigStore}
import net.lshift.diffa.schema.servicelimits._
import org.codehaus.jackson.JsonGenerator
import org.joda.time.DateTime

/**
 * Local in-memory implementation of the DiagnosticsManager.
 *
 *   TODO: Release resources when pair is removed
 */
class LocalDiagnosticsManager(systemConfigStore:SystemConfigStore,
                              domainConfigStore:DomainConfigStore,
                              limits:PairServiceLimitsView,
                              explainLogStore: ExplainLogStore)
    extends DiagnosticsManager
    with PairScanListener
    with AgentLifecycleAware {

  private val pairs = HashMap[DiffaPairRef, PairDiagnostics]()

  def getPairFromRef(ref: DiffaPairRef) = domainConfigStore.getPairDef(ref.domain, ref.key)

  def logPairEvent(scanId:Option[Long], pair: DiffaPairRef, level: DiagnosticLevel, msg: String) {
    val pairDiag = getOrCreatePair(pair)
    pairDiag.logPairEvent(PairEvent(new DateTime(), level, msg))
  }

  def logPairExplanation(scanId:Option[Long], pair: DiffaPairRef, source:String, msg: String) {
    explainLogStore.logPairExplanation(scanId, pair, source, msg)
  }

  def logPairExplanationAttachment(scanId: Option[Long] = None,
                                   pair: DiffaPairRef,
                                   source: String,
                                   tag: String,
                                   requestTimestamp: DateTime,
                                   f: JsonGenerator => Unit) {

    explainLogStore.logPairExplanationAttachment(scanId, pair, source, tag, requestTimestamp, f)
  }

  def queryEvents(pair:DiffaPairRef, maxEvents: Int) = {
    pairs.synchronized { pairs.get(pair) } match {
      case None           => Seq()
      case Some(pairDiag) => pairDiag.queryEvents(maxEvents)
    }
  }

  def retrievePairScanStatesForDomain(domain:String) = {
    val domainPairs = domainConfigStore.listPairs(domain)

    pairs.synchronized {
      domainPairs.map(p => pairs.get(DiffaPairRef(p.key, domain)) match {
        case None           => p.key -> PairScanState.UNKNOWN
        case Some(pairDiag) => p.key -> pairDiag.scanState
      }).toMap
    }
  }

  def pairScanStateChanged(pair: DiffaPairRef, scanState: PairScanState) = pairs.synchronized {
    val pairDiag = getOrCreatePair(pair)
    pairDiag.scanState = scanState
  }

  /**
   * When pairs are deleted, we stop tracking their status in the pair scan map.
   */
  def onDeletePair(pair:DiffaPairRef) {
    pairs.synchronized {
      pairs.remove(pair)
    }
  }

  
  //
  // Lifecycle Management
  //

  override def onAgentInstantiationCompleted(nc: NotificationCentre) {
    nc.registerForPairScanEvents(this)
  }


  //
  // Internals
  //

  private def getOrCreatePair(pair:DiffaPairRef) =
    pairs.synchronized { pairs.getOrElseUpdate(pair, new PairDiagnostics(pair)) }

  private def maybeGetPair(pair:DiffaPairRef) =
    pairs.synchronized { pairs.get(pair) }

  private class PairDiagnostics(pair:DiffaPairRef) {

    private val log = ListBuffer[PairEvent]()

    var scanState:PairScanState = PairScanState.UNKNOWN

    private def getEventBufferSize = limits.getEffectiveLimitByNameForPair(pair.domain, pair.key, DiagnosticEventBufferSize)

    def logPairEvent(evt:PairEvent) = log.synchronized {
      log += evt

      val drop = log.length - getEventBufferSize
      if (drop > 0)
        log.remove(0, drop)
    }

    def queryEvents(maxEvents:Int):Seq[PairEvent] = log.synchronized {
      val startIdx = log.length - maxEvents
      if (startIdx < 0) {
        log.toSeq
      } else {
        log.slice(startIdx, log.length).toSeq
      }
    }
  }

}
