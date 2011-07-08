package net.lshift.diffa.kernel.diag

/**
 * Manager responsible for collecting and providing access to diagnostic information within the system. Diagnostics
 * recorded via this manager are intended for end-user consumption - it does not supersede or replace internal logging,
 * but instead supplements it with a more "user-accessible" view.
 */
trait DiagnosticsManager {
  /**
   * Logs an event relevant to a given pair.
   */
  def logPairEvent(level:DiagnosticLevel, pair:String, msg:String)
}

abstract sealed class DiagnosticLevel
case object TraceLevel extends DiagnosticLevel
case object InfoLevel extends DiagnosticLevel
case object ErrorLevel extends DiagnosticLevel