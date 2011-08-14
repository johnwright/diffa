package net.lshift.diffa.kernel.differencing

import org.junit.Assume._
import org.junit.Assert._
import org.hamcrest.CoreMatchers._
import org.junit.Test
import net.lshift.diffa.kernel.events.VersionID
import org.hibernate.cfg.Configuration
import net.lshift.diffa.kernel.config.{HibernateConfigStorePreparationStep, DiffaPairRef}
import org.apache.commons.io.FileUtils
import java.io.File
import org.joda.time.{Interval, DateTime}

/**
 * Performance test for the domain cache.
 */
class DomainCachePerfTest {
  assumeThat(System.getProperty("diffa.perftest"), is(equalTo("1")))

  @Test
  def differenceInsertionShouldBeConstantTime() {
    val pair = DiffaPairRef(key = "pair", domain = "domain")

    runPerformanceTest("insert", 4) { case (count, cache) =>
      timed(() => {
        for (j <- 0L until count) {
          cache.addReportableUnmatchedEvent(VersionID(pair, "id" + j), new DateTime, "uV", "dV", new DateTime)
        }
      })
    }
  }

  @Test
  def differenceUpgradingShouldBeConstantTime() {
    val pair = DiffaPairRef(key = "pair", domain = "domain")

    runPerformanceTest("upgrade", 4) { case (count, cache) =>
      timed(() => {
        for (j <- 0L until count) {
          cache.addPendingUnmatchedEvent(VersionID(pair, "id" + j), new DateTime, "uV", "dV", new DateTime)
        }
        for (j <- 0L until count) {
          cache.upgradePendingUnmatchedEvent(VersionID(pair, "id" + j))
        }
      })
    }
  }

  @Test
  def matchInsertionShouldBeConstantTime() {
    val pair = DiffaPairRef(key = "pair", domain = "domain")

    runPerformanceTest("matching", 4) { case (count, cache) =>
      for (j <- 0L until count) {
        cache.addReportableUnmatchedEvent(VersionID(pair, "id" + j), new DateTime, "uV", "dV", new DateTime)
      }
      timed(() => {
        for (j <- 0L until count) {
          cache.addMatchedEvent(VersionID(pair, "id" + j), "uV")
        }
      })
    }
  }

  @Test
  def differenceQueryShouldGrowLinearly() {
    val pair = DiffaPairRef(key = "pair", domain = "domain")
    val pair2 = DiffaPairRef(key = "pair2", domain = "domain")

    runPerformanceTest("difference-query", 4) { case (count, cache) =>
      for (j <- 0L until count) {
        cache.addReportableUnmatchedEvent(VersionID(pair, "id" + j), new DateTime, "uV", "dV", new DateTime)
        cache.addReportableUnmatchedEvent(VersionID(pair2, "id" + j), new DateTime, "uV", "dV", new DateTime)
      }
      timed(() => {
        cache.retrievePagedEvents(pair.key, new Interval(new DateTime().minusHours(2), new DateTime().plusHours(2)),
          0, count.asInstanceOf[Int])
      })
    }
  }

  //
  // Support Methods
  //

  def runPerformanceTest(name:String, growth:Int)(f:(Long, DomainCache) => Long) {
    val caches = (1 until (growth+1)).map(i => i -> createCache(name + "-" + i))

    // Run the tests, and record the cost per operation at each growth rate
    println("Events,Total,Per Event")
    val costs = caches.map { case(i, cache) =>
      val insertCount = scala.math.pow(10.0, i.asInstanceOf[Double]).asInstanceOf[Long]

      val duration = f(insertCount, cache)
      val costPerOp = duration.asInstanceOf[Double] / insertCount

      println(insertCount + "," + duration + "," + costPerOp)
      i -> costPerOp
    }

    // Ensure that no operation cost exceeds the cost for any smaller run by more than 20%
    (2 until (growth+1)).foreach(i => {
      val (currentIdx, currentCost) = costs(i - 1)

      costs.slice(0, i).foreach { case (testIdx, cost) =>
        assertFalse(
          "Cost %s at index %s exceeded previous cost %s at index %s by more than 80%%".format(
            currentCost, currentIdx, cost, testIdx
          ),
          currentCost > (cost*1.8))
      }
    })
  }

  def timed(f:() => Unit) = {
    val startTime = System.currentTimeMillis()
    f()
    val endTime = System.currentTimeMillis()
    endTime - startTime
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
