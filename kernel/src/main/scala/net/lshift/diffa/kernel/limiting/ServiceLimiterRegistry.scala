package net.lshift.diffa.kernel.limiting

import net.lshift.diffa.kernel.config.ServiceLimit
import java.util.concurrent.ConcurrentHashMap
import net.lshift.diffa.kernel.util.{Lazy, Registry}
import org.apache.http.annotation.ThreadSafe

case class ServiceLimiterKey(limit: ServiceLimit, domain: Option[String], pair: Option[String]) {
  override def hashCode = 31 * (31 * (31 + limit.hashCode) + domain.getOrElse("__").hashCode) +
    pair.getOrElse("__").hashCode
}

object ServiceLimiterRegistry extends Registry[ServiceLimiterKey, Limiter] {
  val hashMap = new ConcurrentHashMap[ServiceLimiterKey, Lazy[Limiter]]()

  @ThreadSafe
  def get(key: ServiceLimiterKey, factory: () => Limiter): Limiter = {
    val newLimiter = new Lazy(factory())
    val oldLimiter = hashMap.putIfAbsent(key, newLimiter)

    if (oldLimiter == null) {
      newLimiter()
    } else {
      oldLimiter()
    }
  }
}
