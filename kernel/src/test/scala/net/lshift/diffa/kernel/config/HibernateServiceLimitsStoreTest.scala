package net.lshift.diffa.kernel.config

import net.lshift.diffa.kernel.StoreReferenceContainer
import org.junit.{AfterClass, Before, Ignore, Test}

import org.junit.Assert.assertEquals
import org.junit.Assert
import net.lshift.diffa.kernel.frontend.{PairDef, EndpointDef}

/**
 */
class HibernateServiceLimitsStoreTest {
  private val testDomain = Domain("diffa-test-domain")
  private val testPair = DiffaPair(key = "diffa-test-pair", domain = testDomain)
  private val testLimit = ServiceLimitDefinitions("dummyLimit", "A limit that is just for testing")

  private val storeReferences = HibernateServiceLimitsStoreTest.storeReferences
  private val serviceLimitsStore = storeReferences.serviceLimitsStore

  @Before
  def prepareStores {
    val upstream = EndpointDef(name = "upstream")
    val downstream = EndpointDef(name = "downstream")
    val pair = PairDef(key = testPair.key, versionPolicyName = "same",
      upstreamName = upstream.name,
      downstreamName = downstream.name)

    storeReferences.clearConfiguration(testDomain.name)
    storeReferences.systemConfigStore.createOrUpdateDomain(testDomain)
    storeReferences.domainConfigStore.createOrUpdateEndpoint(testDomain.name, upstream)
    storeReferences.domainConfigStore.createOrUpdateEndpoint(testDomain.name, downstream)
    storeReferences.domainConfigStore.createOrUpdatePair(testDomain.name, pair)
    serviceLimitsStore.defineLimit(testLimit.limitName, testLimit.limitDescription)
  }

  @Test
  def givenExistingDependentsWhenSystemHardLimitConfiguredToValidValueNotLessThanDependentLimitsThenLimitShouldBeAppliedAndNoDependentLimitsChanged {
    val (limitName, initialLimit, newLimitValue, depLimit) = (testLimit.limitName, 11, 10, 10)
    // Given
    setAllLimits(limitName, initialLimit, depLimit)

    // When
    serviceLimitsStore.setSystemHardLimit(limitName, newLimitValue)

    // Then
    val (systemHardLimit, systemDefaultLimit, domainHardLimit, domainDefaultLimit, pairLimit) =
      limitValuesForPairByName(testPair, limitName)

    assertEquals(newLimitValue, systemHardLimit)
    assertEquals(depLimit, systemDefaultLimit)
    assertEquals(depLimit, domainHardLimit)
    assertEquals(depLimit, domainDefaultLimit)
    assertEquals(depLimit, pairLimit)
  }

  @Test
  def givenExistingDependentsWhenSystemHardLimitConfiguredToValidValueLessThanDependentLimitsThenLimitShouldBeAppliedAndDependentLimitsLowered {
    val (limitName, initialLimit, newLimitValue, depLimit) = (testLimit.limitName, 11, 9, 10)
    // Given
    setAllLimits(limitName, initialLimit, depLimit)

    // When
    serviceLimitsStore.setSystemHardLimit(limitName, newLimitValue)

    // Then
    val (systemHardLimit, systemDefaultLimit, domainHardLimit, domainDefaultLimit, pairLimit) =
      limitValuesForPairByName(testPair, limitName)

    assertEquals(newLimitValue, systemHardLimit)
    assertEquals(newLimitValue, systemDefaultLimit)
    assertEquals(newLimitValue, domainHardLimit)
    assertEquals(newLimitValue, domainDefaultLimit)
    assertEquals(newLimitValue, pairLimit)
  }

  @Test(expected = classOf[Exception])
  def whenSystemHardLimitConfiguredToInvalidValueThenExceptionThrownVerifyNoLimitChange {
    // Given
    val (limitName, oldLimit) = (testLimit.limitName, 10)
    serviceLimitsStore.setSystemHardLimit(limitName, oldLimit)

    // When
    try {
      serviceLimitsStore.setSystemHardLimit(testLimit.limitName, ServiceLimit.UNLIMITED - 1)
    } catch {
      case ex =>
        // Verify
        assertEquals(serviceLimitsStore.getSystemHardLimitForName(limitName), oldLimit)
        // Then
        throw ex
    }
  }

  @Test
  def givenExistingDependentsWhenDomainScopedHardLimitConfiguredToValidValueNotLessThanDependentLimitsThenLimitShouldBeAppliedAndNoDependentLimitsChanged {
    val (domainName, limitName, initialLimit, newLimitValue, depLimit) = (testDomain.name, testLimit.limitName, 11, 10, 10)
    // Given
    serviceLimitsStore.setDomainHardLimit(domainName, limitName, initialLimit)
    serviceLimitsStore.setDomainDefaultLimit(domainName, limitName, depLimit)
    serviceLimitsStore.setPairLimit(domainName, testPair.key, limitName, depLimit)

    // When
    serviceLimitsStore.setDomainHardLimit(domainName, limitName, newLimitValue)

    // Then
    val (_, _, domainHardLimit, domainDefaultLimit, pairLimit) =
      limitValuesForPairByName(testPair, limitName)

    assertEquals(newLimitValue, domainHardLimit)
    assertEquals(depLimit, domainDefaultLimit)
    assertEquals(depLimit, pairLimit)
  }

  @Test
  def givenExistingDependentsWhenDomainScopedHardLimitConfiguredToValidValueLessThanDependentLimitsThenLimitShouldBeAppliedAndDependentLimitsLowered {
    val (domainName, limitName, initialLimit, newLimitValue, depLimit) = (testDomain.name, testLimit.limitName, 11, 9, 10)
    // Given
    serviceLimitsStore.setDomainHardLimit(domainName, limitName, initialLimit)
    serviceLimitsStore.setDomainDefaultLimit(domainName, limitName, depLimit)
    serviceLimitsStore.setPairLimit(domainName, testPair.key, limitName, depLimit)

    // When
    serviceLimitsStore.setDomainHardLimit(domainName, limitName, newLimitValue)

    // Then
    val (_, _, domainHardLimit, domainDefaultLimit, pairLimit) =
      limitValuesForPairByName(testPair, limitName)

    assertEquals(newLimitValue, domainHardLimit)
    assertEquals(newLimitValue, domainDefaultLimit)
    assertEquals(newLimitValue, pairLimit)
  }

  private def setAllLimits(limitName: String, sysHardLimitValue: Int, otherLimitsValue: Int) {
    serviceLimitsStore.setSystemHardLimit(limitName, sysHardLimitValue)
    serviceLimitsStore.setSystemDefaultLimit(limitName, otherLimitsValue)
    serviceLimitsStore.setDomainHardLimit(testDomain.name, limitName, otherLimitsValue)
    serviceLimitsStore.setDomainDefaultLimit(testDomain.name, limitName, otherLimitsValue)
    serviceLimitsStore.setPairLimit(testDomain.name, testPair.key, limitName, otherLimitsValue)
  }

  private def limitValuesForPairByName(pair: DiffaPair, limitName: String) = {
    val systemHardLimit = serviceLimitsStore.getSystemHardLimitForName(limitName)
    val systemDefaultLimit = serviceLimitsStore.getSystemDefaultLimitForName(limitName)
    val domainHardLimit = serviceLimitsStore.getDomainHardLimitForDomainAndName(pair.domain.name, limitName)
    val domainDefaultLimit = serviceLimitsStore.getDomainDefaultLimitForDomainAndName(pair.domain.name, limitName)
    val pairLimit = serviceLimitsStore.getPairLimitForPairAndName(pair.domain.name, pair.key, limitName)

    (systemHardLimit, systemDefaultLimit, domainHardLimit, domainDefaultLimit, pairLimit)
  }
}

object HibernateServiceLimitsStoreTest {
  private[HibernateServiceLimitsStoreTest] val env = TestDatabaseEnvironments.uniqueEnvironment("target/serviceLimitsStore")

  private[HibernateServiceLimitsStoreTest] val storeReferences =
    StoreReferenceContainer.withCleanDatabaseEnvironment(env)

  @AfterClass
  def cleanupStores {
    storeReferences.tearDown
  }
}