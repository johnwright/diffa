package net.lshift.diffa.kernel.differencing

import org.hibernate.cfg.Configuration
import net.lshift.diffa.kernel.config.HibernateConfigStorePreparationStep
import org.junit.Before

/**
 * Test cases for the HibernateDomainCache.
 */
class HibernateDomainCacheTest extends LocalDomainCacheTest {
  override val cache:DomainCache = HibernateDomainCacheTest.domainCache

  @Before
  def clear() {
    HibernateDomainCacheTest.clearAll()
  }
}

object HibernateDomainCacheTest {
  private val config =
      new Configuration().
        addResource("net/lshift/diffa/kernel/config/Config.hbm.xml").
        addResource("net/lshift/diffa/kernel/differencing/DifferenceEvents.hbm.xml").
        setProperty("hibernate.dialect", "org.hibernate.dialect.DerbyDialect").
        setProperty("hibernate.connection.url", "jdbc:derby:target/domainCache;create=true").
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
  val domainCache = domainCacheProvider.retrieveOrAllocateCache("domain")

  def clearAll() {
    domainCacheProvider.clearAllDifferences
  }
}