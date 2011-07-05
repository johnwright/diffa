package net.lshift.diffa.participant.correlation;

/**
 * Handler interface that can be implemented in client libraries to allow for correlated version recovery.
 */
public interface VersioningParticipantHandler {
  /**
   * Requests that the participant generate a processing response based on the given incoming entity data. The downstream
   * participant is not expected to re-admit the data into its system - it should simply parse the data so as to be able
   * to determine it's own version information.
   * @param entityBody the body of the entity as provided by the upstream.
   * @return a processing response detailed version information.
   */
  ProcessingResponse generateVersion(String entityBody);
}
