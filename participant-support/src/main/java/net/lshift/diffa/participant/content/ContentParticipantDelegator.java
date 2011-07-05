package net.lshift.diffa.participant.content;

/**
 * Adapter allowing a ContentParticipant to be implemented without requiring it to sub-class the
 * ContentParticipantRequestHandler or the ContentParticipantServlet, and instead be delegated to.
 */
public class ContentParticipantDelegator extends ContentParticipantRequestHandler {
  private final ContentParticipantHandler handler;

  public ContentParticipantDelegator(ContentParticipantHandler handler) {
    this.handler = handler;
  }

  @Override
  protected String retrieveContent(String identifier) {
    return handler.retrieveContent(identifier);
  }
}
