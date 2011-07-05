package net.lshift.diffa.kernel.participants

import java.io.Closeable

/**
 * Reference to a content participant.
 */
trait ContentParticipantRef extends Closeable {
  /**
   * Requests that the participant return a serialized form of the item with the given identifier.
   */
  def retrieveContent(identifier:String): String
}

/**
 * Factory for creating content participant references.
 */
trait ContentParticipantFactory extends AddressDrivenFactory[ContentParticipantRef]