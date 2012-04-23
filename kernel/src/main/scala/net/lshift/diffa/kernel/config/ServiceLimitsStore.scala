package net.lshift.diffa.kernel.config

/**
 * Interface to administration of Service Limits.
 */
trait ServiceLimitsStore {
  def defineLimit(limitName: String, description: String): Unit
  def deleteDomainLimits(domainName: String): Unit
  def deletePairLimitsByDomain(domainName: String): Unit

  def setSystemHardLimit(limitName: String, limitValue: Int): Unit
  def setSystemDefaultLimit(limitName: String, limitValue: Int): Unit
  def setDomainHardLimit(domainName: String, limitName: String, limitValue: Int): Unit
  def setDomainDefaultLimit(domainName: String, limitName: String, limitValue: Int): Unit
  def setPairLimit(domainName: String, pairKey: String, limitName: String, limitValue: Int): Unit
  
  def getSystemHardLimitForName(limitName: String): Option[Int]
  def getSystemDefaultLimitForName(limitName: String): Option[Int]
  def getDomainHardLimitForDomainAndName(domainName: String, limitName: String): Option[Int]
  def getDomainDefaultLimitForDomainAndName(domainName: String, limitName: String): Option[Int]
  def getPairLimitForPairAndName(domainName: String, pairKey: String, limitName: String): Option[Int]

  def getEffectiveLimitByName(limitName: String): Int
  def getEffectiveLimitByNameForDomain(limitName: String, domainName: String): Int
  def getEffectiveLimitByNameForPair(limitName: String, domainName: String, pairKey: String): Int
}
