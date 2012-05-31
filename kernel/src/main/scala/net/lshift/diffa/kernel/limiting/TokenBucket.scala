package net.lshift.diffa.kernel.limiting

object TokenBucket {
  def apply(params: TokenBucketParameters, clock: Clock) =
    new FixedIntervalTokenBucket(params, clock)
}

trait TokenBucket {
  def tryConsume: Boolean
  def refill()
}

class FixedIntervalTokenBucket(params: TokenBucketParameters, clock: Clock) {
  private var lastRefillTime = clock.currentTimeMillis
  private var volume: Long = params.initialVolume

  def tryConsume: Boolean = {
    synchronized {
      refill()

      if (volume > 0) {
        volume -= 1
        true
      } else {
        false
      }
    }
  }

  def refill() {
    val now = clock.currentTimeMillis
    volume = math.min(
      volume + params.refillAmount * floor((now - lastRefillTime)/params.refillInterval),
      params.capacity)
    lastRefillTime = now
  }

  private def floor(d: Double): Long = math.round(math.floor(d))
}

trait TokenBucketParameters {
  def capacity: Long
  def initialVolume: Long = 0L
  def refillInterval: Long = 1000L // milliseconds
  def refillAmount: Long = 1L
}
