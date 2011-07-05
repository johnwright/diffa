package net.lshift.diffa.participant.scanning;

/**
 * Handler interface that can be implemented in client libraries to allow for content queries.
 */
public interface ContentParticipantHandler {
  /**
   * Retrieve the content for a given identifier.
   * @param identifier the entity identifier.
   * @return the entity content, or null if the entity is unknown.
   */
  String retrieveContent(String identifier);
}
