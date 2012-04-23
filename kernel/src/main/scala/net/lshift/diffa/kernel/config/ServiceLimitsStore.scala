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
  
  def getSystemHardLimitForName(limitName: String): Int
  def getSystemDefaultLimitForName(limitName: String): Int
  def getDomainHardLimitForDomainAndName(domainName: String, limitName: String): Int
  def getDomainDefaultLimitForDomainAndName(domainName: String, limitName: String): Int
  def getPairLimitForPairAndName(domainName: String, pairKey: String, limitName: String): Int
}
