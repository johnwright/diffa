package net.lshift.diffa.kernel.diag;

/**
 * Describes the level for a log event. See <code>java.util.logging.Level</code> for appropriate theory.
 */
public enum DiagnosticLevel {
  /**
   * Trace event. Tracks relatively low level activity that is generally useful for understanding internal system
   * processes.
   */
  Trace,

  /**
   * Information-level event. Tracks a high level activity that should generally be of user interest.
   */
  Info,

  /**
   * Error event. Tracks an error that has occurred within a process that might be of interest to a user attempting
   * to understand an operation failure.
   */
  Error
}
