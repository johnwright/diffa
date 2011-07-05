package net.lshift.diffa.kernel.participants

import java.io.Closeable
import net.lshift.diffa.participant.scanning.ProcessingResponse

/**
 * Reference to a version generating participant. This allows for version information to be recovered in a correlated
 * version system.
 */
trait VersioningParticipantRef extends Closeable {
  /**
   * Requests that the participant generate a processing response based on the given incoming entity data. The downstream
   * participant is not expected to re-admit the data into its system - it should simply parse the data so as to be able
   * to determine it's own version information.
   */
  def generateVersion(entityBody: String): ProcessingResponse
}

/**
 * Factory for creating version generation participants.
 */
trait VersioningParticipantFactory extends AddressDrivenFactory[VersioningParticipantRef]