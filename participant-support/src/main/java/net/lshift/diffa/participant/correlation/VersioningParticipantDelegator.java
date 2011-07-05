package net.lshift.diffa.participant.correlation;

/**
 * Adapter allowing a ContentParticipant to be implemented without requiring it to sub-class the
 * ContentParticipantRequestHandler or the ContentParticipantServlet, and instead be delegated to.
 */
public class VersioningParticipantDelegator extends VersioningParticipantRequestHandler {
  private final VersioningParticipantHandler handler;

  public VersioningParticipantDelegator(VersioningParticipantHandler handler) {
    this.handler = handler;
  }

  @Override
  public ProcessingResponse generateVersion(String entityBody) {
    return handler.generateVersion(entityBody);
  }
}
