package net.lshift.diffa.kernel.diag

import org.joda.time.DateTime
import reflect.BeanProperty
import net.lshift.diffa.kernel.config.{Pair => DiffaPair}

/**
 * Manager responsible for collecting and providing access to diagnostic information within the system. Diagnostics
 * recorded via this manager are intended for end-user consumption - it does not supersede or replace internal logging,
 * but instead supplements it with a more "user-accessible" view.
 */
trait DiagnosticsManager {
  /**
   * Logs an event relevant to a given pair.
   */
  def logPairEvent(level:DiagnosticLevel, pair:DiffaPair, msg:String)

  /**
   * Queries for known events about the given pair.
   */
  def queryEvents(pair:DiffaPair, maxEvents:Int):Seq[PairEvent]
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