package net.lshift.diffa.kernel.util.cache

import org.junit.Assert._
import org.junit.runner.RunWith
import org.junit.experimental.theories.{DataPoint, Theories, Theory}

@RunWith(classOf[Theories])
class CacheProviderTest {

  @Theory
  def shouldProvideReadThroughCaching(provider:CacheProvider) {
    val cache = provider.getCachedMap[String,String]("some-cache")

    cache.evictAll
    assertEquals(0, cache.size)

    val underlying = new UnderlyingDataSource("some-value")
    val unCachedResponse = cache.readThrough("some_key", underlying.getData)
    assertEquals("some-value", unCachedResponse)
    val cachedResponse = cache.readThrough("some_key", underlying.getData)
    assertEquals("some-value", cachedResponse)
    assertEquals(1, underlying.invocations)
  }

}

object CacheProviderTest {
  @DataPoint def hazelcast = new HazelcastCacheProvider
}

class UnderlyingDataSource(responseValue:String) {

  var invocations = 0

  def getData() = {
    invocations += 1
    responseValue
  }
}
