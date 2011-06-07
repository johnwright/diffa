package net.lshift.diffa.participant.scanning;

/**
* Exception indicating that getDigest has already been called on a DigestBuilder, so it can no longer have
* items added.
*/
class SealedBucketException extends RuntimeException {
  public SealedBucketException(String vsn, String name) {
    super(vsn + " -> " + name);
  }
}
