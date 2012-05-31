package net.lshift.diffa.kernel.limiting

import net.lshift.diffa.kernel.config.DomainServiceLimitsView
import net.lshift.diffa.kernel.config.limits.{Unlimited, ChangeEventRate}

class ServiceLimitsDomainRateLimiterFactory(domainLimitsView: DomainServiceLimitsView) extends DomainRateLimiterFactory {
  def createRateLimiter(domainName: String): RateLimiter = new RateLimiter(
    () => domainLimitsView.getEffectiveLimitByNameForDomain(domainName, ChangeEventRate)
  )
}

trait DomainRateLimiterFactory extends RateLimiterFactory[String]

trait RateLimiterFactory[T] {
  def createRateLimiter(t: T): RateLimiter
}

/**
 * Limit the rate at which actions of the specified type are permitted.
 * Any action that should be rate limited should call RateLimiter.accept before executing the
 * rate limited action.
 */
class RateLimiter(eventsPerSecondFn: () => Int, clock: Clock = SystemClock) extends Limiter {
  private val params = new TokenBucketParameters {
    def capacity = eventsPerSecondFn()
    override def initialVolume = eventsPerSecondFn()
    override def refillInterval = 1000L
    override def refillAmount = eventsPerSecondFn()
  }

  private val tokenBucket = TokenBucket(params, clock)
  private val acceptFn: () => Boolean = if (eventsPerSecondFn() == Unlimited.value) {
    () => true
  } else {
    () => tokenBucket.tryConsume
  }

  def accept() = acceptFn()
}

trait Limiter {
  def accept(): Boolean
}

trait TypedLimiter[ActionType] {
  def accept(action: ActionType): Boolean
}
