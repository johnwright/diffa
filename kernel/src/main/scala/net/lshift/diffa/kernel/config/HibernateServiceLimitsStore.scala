package net.lshift.diffa.kernel.config

import net.lshift.diffa.kernel.util.SessionHelper._
import org.hibernate.SessionFactory
import net.lshift.diffa.kernel.util.{CachedMap, CacheProvider, HibernateQueryUtils}


class HibernateServiceLimitsStore(val sessionFactory: SessionFactory, val cacheProvider:CacheProvider)
  extends ServiceLimitsStore
  with HibernateQueryUtils {

  val pairScopedLimitCache = cacheProvider.getCachedMap[Option[Int]]("service.limits.pair.scope")
  val domainScopedDefaultValues = cacheProvider.getCachedMap[Option[Int]]("service.defaults.domain.scope")
  val domainScopedHardLimits = cacheProvider.getCachedMap[Option[Int]]("service.limits.domain.scope")
  val systemScopedDefaultValues = cacheProvider.getCachedMap[Option[Int]]("service.defaults.system.scope")
  val systemScopedHardLimits = cacheProvider.getCachedMap[Option[Int]]("service.limits.system.scope")

  private def getPairScopedKey(domain:String, pair:String, limit:String) = "%s.%s.%s".format(domain,pair,limit)
  private def getDomainScopedKey(domain:String, limit:String) = "%s.%s".format(domain,limit)

  private def validate(limitValue: Int) {
    if (limitValue < 0 && limitValue != ServiceLimit.UNLIMITED)
      throw new Exception("Invalid limit value")
  }

  def defineLimit(limitName: String, description: String) {
    createOrUpdate[ServiceLimitDefinitions](
      () => ServiceLimitDefinitions(limitName, description),
      () => limitName,
      old => {
        old.limitName = limitName
        old.limitDescription = description
      }
    )
  }

  def deleteDomainLimits(domainName: String) {
    def eviction() = {
      domainScopedHardLimits.evictByPrefix(domainName)
      domainScopedDefaultValues.evictByPrefix(domainName)
    }
    deletePairLimitsByDomain(domainName)
    deleteLimitsByDomain[DomainServiceLimits](domainName, "domainServiceLimitsByDomain", eviction)
  }

  def deletePairLimitsByDomain(domainName: String) {
    def eviction() = {
      pairScopedLimitCache.evictByPrefix(domainName)
    }
    deleteLimitsByDomain[PairServiceLimits](domainName, "pairServiceLimitsByDomain", eviction)
  }

  private def deleteLimitsByDomain[T](domainName: String, queryName: String, cacheEviction:Function0[_]) {
    sessionFactory.withSession(session => {
      listQuery[T](
        session, queryName, Map("domain_name" -> domainName)
      ).foreach(
        session.delete
      )
    }, cacheEviction)
  }

  def setSystemHardLimit(limitName: String, limitValue: Int) {
    validate(limitValue)
    setSystemLimit(limitName, limitValue, old => old.hardLimit = limitValue)

    cascadeLimitToSystemDefault(limitName, limitValue)
    cascadeLimitToDomainHardLimit(limitName, limitValue)
  }

  def setDomainHardLimit(domainName: String, limitName: String, limitValue: Int) {
    validate(limitValue)
    setDomainLimit(domainName, limitName, limitValue, old => old.hardLimit = limitValue)

    cascadeLimitToDomainDefaultLimit(limitName, limitValue)
    cascadeLimitToPair(limitName, limitValue)
  }

  def setSystemDefaultLimit(limitName: String, limitValue: Int) {
    setSystemLimit(limitName, limitValue, old => old.defaultLimit = limitValue)
  }

  def setDomainDefaultLimit(domainName: String, limitName: String, limitValue: Int) {
    setDomainLimit(domainName, limitName, limitValue, old => old.defaultLimit = limitValue)
  }

  def setPairLimit(domainName: String, pairKey: String, limitName: String, limitValue: Int) {
    val domain = getDomain(domainName)
    val pair = DiffaPair(key = pairKey, domain = domain)

    createOrUpdate[PairServiceLimits](
      () => PairServiceLimits(domain, pair, limitName, limitValue),
      () => PairScopedLimit(limitName, pair),
      old => old.limitValue = limitValue
    )
  }

  def getSystemHardLimitForName(limitName: String) = {
    systemScopedHardLimits.readThrough(
      limitName,
      () => getLimit("systemHardLimitByName", Map("limit_name" -> limitName))
    )
  }

  def getSystemDefaultLimitForName(limitName: String) = {
    systemScopedDefaultValues.readThrough(
      limitName,
      () => getLimit("systemDefaultLimitByName", Map("limit_name" -> limitName))
    )
  }

  def getDomainHardLimitForDomainAndName(domainName: String, limitName: String) = {
    domainScopedHardLimits.readThrough(
      getDomainScopedKey(domainName,limitName),
      () => getLimit("domainHardLimitByDomainAndName", Map("limit_name" -> limitName, "domain_name" -> domainName))
    )
  }

  def getDomainDefaultLimitForDomainAndName(domainName: String, limitName: String) = {
    domainScopedDefaultValues.readThrough(
      getDomainScopedKey(domainName,limitName),
      () => getLimit("domainDefaultLimitByDomainAndName",Map("limit_name" -> limitName, "domain_name" -> domainName))
    )
  }

  def getPairLimitForPairAndName(domainName: String, pairKey: String, limitName: String) = {
    pairScopedLimitCache.readThrough(
      getPairScopedKey(domainName, pairKey, limitName),
      () => getLimit("pairLimitByPairAndName", Map("limit_name" -> limitName, "domain_name" -> domainName, "pair_key" -> pairKey))
    )
  }

  def getEffectiveLimitByNameForPair(limitName: String, domainName: String, pairKey: String) =
    getPairLimitForPairAndName(domainName, pairKey, limitName).getOrElse(
      getEffectiveLimitByNameForDomain(limitName, domainName))

  def getEffectiveLimitByNameForDomain(limitName: String, domainName: String) =
    getDomainDefaultLimitForDomainAndName(domainName, limitName).getOrElse(
      getEffectiveLimitByName(limitName))

  def getEffectiveLimitByName(limitName: String) =
    getSystemDefaultLimitForName(limitName).getOrElse(
      ServiceLimit.UNLIMITED)

  private def getLimit(queryName: String, params: Map[String, String]) = sessionFactory.withSession(session =>
    singleQueryOpt[Int](
      session, queryName, params
    ))

  private def cascadeLimit(currentLimit: Int, setLimitValue: (String, Int) => Unit,
                           limitName: String, newLimit: Int) {
    if (currentLimit > newLimit) {
      setLimitValue(limitName, newLimit)
    }
  }

  private def cascadeLimitToSystemDefault(limitName: String, limitValue: Int) {
    cascadeLimit(
      getEffectiveLimitByName(limitName),
      setSystemDefaultLimit,
      limitName, limitValue)
  }
  
  private def cascadeLimitToDomainHardLimit(limitName: String, limitValue: Int) {
    sessionFactory.withSession(
      session => listQuery[DomainServiceLimits](session, "domainServiceLimitsByName", Map("limit_name" -> limitName))
    ).foreach { domainLimit =>
      cascadeLimit(
        domainLimit.hardLimit,
        (limitName, limitValue) => setDomainHardLimit(domainLimit.domain.name, limitName, limitValue),
        limitName, limitValue
      )
    }
  }

  private def cascadeLimitToDomainDefaultLimit(limitName: String, limitValue: Int) {
    sessionFactory.withSession(
      session => listQuery[DomainServiceLimits](session, "domainServiceLimitsByName", Map("limit_name" -> limitName))
    ).foreach { domainLimit =>
      cascadeLimit(
        domainLimit.defaultLimit,
        (limitName, limitValue) => setDomainDefaultLimit(domainLimit.domain.name, limitName, limitValue),
        limitName, limitValue
      )
    }
  }
  
  private def cascadeLimitToPair(limitName: String, limitValue: Int) {
    sessionFactory.withSession(
      session => listQuery[PairServiceLimits](session, "pairServiceLimitsByName", Map("limit_name" -> limitName))
    ).foreach { pairLimit =>
      cascadeLimit(
        pairLimit.limitValue,
        (limitName, limitValue) => setPairLimit(pairLimit.pair.domain.name, pairLimit.pair.key, limitName, limitValue),
        limitName, limitValue
      )
    }
  }

  private def createOrUpdate[T: ClassManifest](createLimit: () => T, searchKey: () => java.io.Serializable, updateLimit: T => Unit) {
    sessionFactory.withSession(session => {
      val lim = session.get(classManifest[T].erasure, searchKey()) match {
        case null =>
          createLimit()
        case old: T => {
          updateLimit(old)
          old
        }
      }
      session.saveOrUpdate(lim)
    })
  }

  private def setSystemLimit(limitName: String, limitValue: Int, updateLimit: SystemServiceLimits => Unit) {
    createOrUpdate[SystemServiceLimits](
      () => SystemServiceLimits(limitName, limitValue, limitValue),
      () => limitName,
      updateLimit
    )
  }

  private def setDomainLimit(domainName: String, limitName: String, limitValue: Int, updateLimit: DomainServiceLimits => Unit) {
    val domain = getDomain(domainName)
    createOrUpdate[DomainServiceLimits](
      () => DomainServiceLimits(domain, limitName, limitValue, limitValue),
      () => DomainScopedLimit(limitName, domain),
      updateLimit
    )
  }
}
