package net.lshift.diffa.kernel.participants

/**
 * Factory for creating scanning participants.
 */
trait ScanningParticipantFactory extends AddressDrivenFactory {
  /**
   * Queries whether the factory supports the given address/protocol combination.
   */
  def supportsAddress(address:String, protocol:String):Boolean

  /**
   * Creates a scanning participant reference using the given address and protocol. It is expected the factory has
   * already been checked for compatibility via supportsAddress. The behaviour when calling this method without
   * previously checking is undefined, and the factory implementation may return a non-functional proxy.
   */
  def createScanningParticipantRef(address:String, protocol:String): ScanningParticipantRef
}