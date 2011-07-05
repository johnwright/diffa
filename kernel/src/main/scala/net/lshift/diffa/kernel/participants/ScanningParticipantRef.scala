package net.lshift.diffa.kernel.participants

import net.lshift.diffa.participant.scanning.ScanResultEntry
import java.io.Closeable

/**
 * Reference to a scanning participant.
 */
trait ScanningParticipantRef extends Closeable {
  /**
   * Scans this participant with the given constraints and aggregations.
   */
  def scan(constraints:Seq[QueryConstraint], aggregations:Map[String, CategoryFunction]): Seq[ScanResultEntry]
}

/**
 * Factory for creating scanning participant references.
 */
trait ScanningParticipantFactory extends AddressDrivenFactory[ScanningParticipantRef]