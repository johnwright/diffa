package net.lshift.diffa.kernel.util.cache

import org.junit.Assert._
import org.junit.runner.RunWith
import org.junit.experimental.theories.{DataPoint, Theories, Theory}
import reflect.BeanProperty

@RunWith(classOf[Theories])
class CacheProviderTest {

  @Theory
  def shouldProvideReadThroughCaching(provider:CacheProvider) {
    val cache = provider.getCachedMap[String,String]("some-cache")

    reset(cache)

    val underlying = new UnderlyingDataSource("some-value")
    val unCachedResponse = cache.readThrough("some_key", underlying.getData)
    assertEquals("some-value", unCachedResponse)
    val cachedResponse = cache.readThrough("some_key", underlying.getData)
    assertEquals("some-value", cachedResponse)
    assertEquals(1, underlying.invocations)
  }

  @Theory
  def shouldSupportRemovalBasedOnKeys(provider:CacheProvider) {
    val cache = provider.getCachedMap[TestCacheKey,String]("another-cache")

    reset(cache)

    cache.put(TestCacheKey("foo", 1), "first-value")
    cache.put(TestCacheKey("bar", 2), "second-value")

    assertEquals(2, cache.size())
    assertEquals("first-value", cache.get(TestCacheKey("foo", 1)))
    assertEquals("second-value", cache.get(TestCacheKey("bar", 2)))

    cache.subset(TestKeyPredicate("foo")).evictAll

    assertEquals(1, cache.size())
    assertNull(cache.get(TestCacheKey("foo", 1)))
    assertEquals("second-value", cache.get(TestCacheKey("bar", 2)))

  }

  private def reset(cache:CachedMap[_,_]) = {
    cache.evictAll
    assertEquals(0, cache.size)
  }

}

object CacheProviderTest {
  @DataPoint def hazelcast = new HazelcastCacheProvider
}

case class TestCacheKey(
  @BeanProperty var firstAttribute: String = null,
  @BeanProperty var secondAttribute: java.lang.Integer = null) {

  def this() = this(firstAttribute = null)
}

case class TestKeyPredicate(@BeanProperty firstAttribute:String) extends KeyPredicate[TestCacheKey] {
  def this() = this(firstAttribute = null)
  def constrain(key: TestCacheKey) = key.firstAttribute == firstAttribute
}

class UnderlyingDataSource(responseValue:String) {

  var invocations = 0

  def getData() = {
    invocations += 1
    responseValue
  }
}
