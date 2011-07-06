package net.lshift.diffa.kernel.participants

import net.lshift.diffa.participant.scanning.ScanResultEntry
import java.io.Closeable

/**
 * Provides a reference to a scanning participant. An implementation of this will be provided via a
 * ScanningParticipantFactory implementation, and will generally be an accessor to a remote resource. The
 * implementation of this will be responsible for handling argument serialization, RPC execution and result
 * deserialization.
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