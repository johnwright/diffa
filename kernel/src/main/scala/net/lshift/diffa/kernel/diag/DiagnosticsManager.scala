package net.lshift.diffa.kernel.diag

import org.joda.time.DateTime
import reflect.BeanProperty
import net.lshift.diffa.kernel.differencing.PairScanState
import net.lshift.diffa.kernel.config.{DiffaPairRef, Pair => DiffaPair}

/**
 * Manager responsible for collecting and providing access to diagnostic information within the system. Diagnostics
 * recorded via this manager are intended for end-user consumption - it does not supersede or replace internal logging,
 * but instead supplements it with a more "user-accessible" view.
 */
trait DiagnosticsManager {
  /**
   * Logs an event relevant to a given pair.
   */
  def logPairEvent(level:DiagnosticLevel, pair:DiffaPairRef, msg:String)

  /**
   * Logs an explanation event for a pair. Explanations are expected to be highly verbose details about system
   * internals, and the diagnostics manager is responsible for aggregating these into sets for later system analysis.
   */
  def logPairExplanation(pair:DiffaPairRef, msg:String)

  /**
   * Attaches an 'object' that helps explain Diffa behaviour for a pair. The most common object will be responses from
   * participants. The diagnostics manager will store this object alongside explanation information, using the provided
   * object name as a marker.
   */
  def attachPairExplanationObject(pair:DiffaPairRef, objName:String, content:String)

  /**
   * Queries for known events about the given pair.
   */
  def queryEvents(pair:DiffaPairRef, maxEvents:Int):Seq[PairEvent]

  /**
   * Retrieves the scan states for each pair configured within the given domain.
   */
  def retrievePairScanStatesForDomain(domain:String):Map[String, PairScanState]

  /**
   * Informs the diagnostics manager that a pair has been deleted.
   */
  def onDeletePair(pair:DiffaPairRef)
}

/**
 * Describes an event that has occurred for a pair.
 */
case class PairEvent(
  @BeanProperty var timestamp:DateTime = null,
  @BeanProperty var level:DiagnosticLevel = DiagnosticLevel.INFO,
  @BeanProperty var msg:String = null
) {
  def this() = this(timestamp = null)
}