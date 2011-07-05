package net.lshift.diffa.kernel.participants

/**
 * Parent trait inherited by factories that create objects based on addresses and content types.
 */
trait AddressDrivenFactory[T] {
  /**
   * Determines whether this factory accepts addresses of the given form with the given content type.
   */
  def supportsAddress(address:String, contentType:String):Boolean

  /**
   * Creates a participant reference using the given address and protocol. It is expected the factory has
   * already been checked for compatibility via supportsAddress. The behaviour when calling this method without
   * previously checking is undefined, and the factory implementation may return a non-functional proxy.
   */
  def createParticipantRef(address:String, protocol:String): T
}