package net.lshift.diffa.kernel.differencing

import org.junit.Assume._
import org.hamcrest.CoreMatchers._
import org.junit.Test
import net.lshift.diffa.kernel.events.VersionID
import org.joda.time.DateTime
import org.hibernate.cfg.Configuration
import net.lshift.diffa.kernel.config.{HibernateConfigStorePreparationStep, DiffaPairRef}
import org.apache.commons.io.FileUtils
import java.io.File

/**
 * Performance test for the domain cache.
 */
class DomainCachePerfTest {
  assumeThat(System.getProperty("diffa.perftest"), is(equalTo("1")))

  @Test
  def differenceInsertionShouldBeConstantTime() {
    val pair = DiffaPairRef(key = "pair", domain = "domain")

    runPerformanceTest("insert", 3) { case (count, cache) =>
      for (j <- 0L until count) {
        cache.addReportableUnmatchedEvent(VersionID(pair, "id" + j), new DateTime, "uV", "dV", new DateTime)
      }
    }
  }

  @Test
  def differenceUpgradingShouldBeConstantTime() {
    val pair = DiffaPairRef(key = "pair", domain = "domain")

    runPerformanceTest("upgrade", 3) { case (count, cache) =>
      for (j <- 0L until count) {
        cache.addPendingUnmatchedEvent(VersionID(pair, "id" + j), new DateTime, "uV", "dV", new DateTime)
      }
      for (j <- 0L until count) {
        cache.upgradePendingUnmatchedEvent(VersionID(pair, "id" + j))
      }
    }
  }

  //
  // Support Methods
  //

  def runPerformanceTest(name:String, growth:Int)(f:(Long, DomainCache) => Unit) {
    val caches = (1 until growth).map(i => i -> createCache(name + "-" + i))

    println("Events,Total,Per Event")
    caches.foreach { case(i, cache) =>
      val insertCount = scala.math.pow(10.0, i.asInstanceOf[Double]).asInstanceOf[Long]

      val startTime = System.currentTimeMillis()
      f(insertCount, cache)
      val endTime = System.currentTimeMillis()
      val duration = endTime - startTime

      println(insertCount + "," + duration + "," + duration.asInstanceOf[Double] / insertCount)
    }
  }

  def createCache(domain:String) = createPersistentCache(domain)
  def createLocalCache(domain:String) = new LocalDomainCache(domain)
  def createPersistentCache(domain:String) = {
    FileUtils.deleteDirectory(new File("target/domain-" + domain))

    val config =
      new Configuration().
        addResource("net/lshift/diffa/kernel/config/Config.hbm.xml").
        addResource("net/lshift/diffa/kernel/differencing/DifferenceEvents.hbm.xml").
        setProperty("hibernate.dialect", "org.hibernate.dialect.DerbyDialect").
        setProperty("hibernate.connection.url", "jdbc:derby:target/domain-" + domain + ";create=true").
        setProperty("hibernate.connection.driver_class", "org.apache.derby.jdbc.EmbeddedDriver").
        setProperty("hibernate.cache.region.factory_class", "net.sf.ehcache.hibernate.EhCacheRegionFactory").
        setProperty("hibernate.connection.autocommit", "true") // Turn this on to make the tests repeatable,
                                                               // otherwise the preparation step will not get committed

    val sessionFactory = {
      val sf = config.buildSessionFactory
      (new HibernateConfigStorePreparationStep).prepare(sf, config)
      sf
    }

    val domainCacheProvider = new HibernateDomainCacheProvider(sessionFactory)
    domainCacheProvider.retrieveOrAllocateCache(domain)
  }
}