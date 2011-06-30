package net.lshift.diffa.kernel.participants

/**
 * Parent trait inherited by factories that create objects based on addresses and content types.
 */
trait AddressDrivenFactory {
  /**
   * Determines whether this factory accepts addresses of the given form with the given content type.
   */
  def supportsAddress(address:String, contentType:String):Boolean
}