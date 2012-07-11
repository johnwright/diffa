package net.lshift.diffa.kernel.config

import net.lshift.diffa.kernel.util.db.{HibernateQueryUtils, DatabaseFacade}
import net.lshift.diffa.schema.servicelimits._
import net.lshift.diffa.schema.hibernate.SessionHelper._
import org.hibernate.SessionFactory
import net.lshift.diffa.schema.jooq.{DatabaseFacade => JooqDatabaseFacade}
import net.lshift.diffa.schema.tables.PairLimits.PAIR_LIMITS
import net.lshift.diffa.schema.tables.DomainLimits.DOMAIN_LIMITS
import net.lshift.diffa.schema.tables.SystemLimits.SYSTEM_LIMITS
import org.jooq.impl.{TableImpl, Factory}
import org.jooq.{Condition, TableField}
import scala.collection.JavaConversions._

class HibernateServiceLimitsStore(val sessionFactory: SessionFactory,
                                  db:DatabaseFacade,
                                  jooq:JooqDatabaseFacade)
  extends ServiceLimitsStore
  with HibernateQueryUtils {

  private def validate(limitValue: Int) {
    if (limitValue < 0 && limitValue != Unlimited.hardLimit)
      throw new Exception("Invalid limit value")
  }

  def defineLimit(limit: ServiceLimit) {
    createOrUpdate[ServiceLimitDefinitions](
      () => ServiceLimitDefinitions(limit.key, limit.description),
      () => limit.key,
      old => {
        old.limitName = limit.key
        old.limitDescription = limit.description
      }
    )
  }

  def deleteDomainLimits(domain: String) = jooq.execute(t => {

    deletePairLimitsByDomainInternal(t, domain)

    t.delete(DOMAIN_LIMITS).
      where(DOMAIN_LIMITS.DOMAIN.equal(domain)).
      execute()
  })

  def deletePairLimitsByDomain(domain: String) = jooq.execute(deletePairLimitsByDomainInternal(_, domain))

  def setSystemHardLimit(limit: ServiceLimit, limitValue: Int)  = jooq.execute(t => {
    validate(limitValue)

    setSystemLimit(limit, limitValue, SYSTEM_LIMITS.HARD_LIMIT)

    cascadeLimitToSystemDefault(limit, limitValue)
    cascadeLimitToDomainHardLimit(limit, limitValue)
  })

  def setDomainHardLimit(domainName: String, limit: ServiceLimit, limitValue: Int) {
    validate(limitValue)
    setDomainLimit(domainName, limit, limitValue, DOMAIN_LIMITS.HARD_LIMIT)

    cascadeLimitToDomainDefaultLimit(limit, limitValue)
    cascadeLimitToPair(limit, limitValue)
  }

  def setSystemDefaultLimit(limit: ServiceLimit, limitValue: Int) {
    setSystemLimit(limit, limitValue, SYSTEM_LIMITS.DEFAULT_LIMIT)
  }

  def setDomainDefaultLimit(domainName: String, limit: ServiceLimit, limitValue: Int) {
    setDomainLimit(domainName, limit, limitValue, DOMAIN_LIMITS.DEFAULT_LIMIT)
  }

  def setPairLimit(domainName: String, pairKey: String, limit: ServiceLimit, limitValue: Int) =
    jooq.execute(setPairLimit(_, domainName, pairKey, limit, limitValue))

  def getSystemHardLimitForName(limit: ServiceLimit) =
    getLimit(SYSTEM_LIMITS.HARD_LIMIT, SYSTEM_LIMITS, SYSTEM_LIMITS.NAME.equal(limit.key))

  def getSystemDefaultLimitForName(limit: ServiceLimit) =
    getLimit(SYSTEM_LIMITS.DEFAULT_LIMIT, SYSTEM_LIMITS, SYSTEM_LIMITS.NAME.equal(limit.key))

  def getDomainHardLimitForDomainAndName(domainName: String, limit: ServiceLimit) =
    getLimit(DOMAIN_LIMITS.HARD_LIMIT, DOMAIN_LIMITS, DOMAIN_LIMITS.NAME.equal(limit.key), DOMAIN_LIMITS.DOMAIN.equal(domainName))

  def getDomainDefaultLimitForDomainAndName(domainName: String, limit: ServiceLimit) =
    getLimit(DOMAIN_LIMITS.DEFAULT_LIMIT, DOMAIN_LIMITS, DOMAIN_LIMITS.NAME.equal(limit.key), DOMAIN_LIMITS.DOMAIN.equal(domainName))

  def getPairLimitForPairAndName(domainName: String, pairKey: String, limit: ServiceLimit) =
    getLimit(PAIR_LIMITS.LIMIT_VALUE, PAIR_LIMITS,
             PAIR_LIMITS.NAME.equal(limit.key), PAIR_LIMITS.DOMAIN.equal(domainName), PAIR_LIMITS.PAIR_KEY.equal(pairKey))

  private def getLimit(limitValue:TableField[_,_], table:TableImpl[_], predicate:Condition*) : Option[Int] = jooq.execute(t => {

    val record = t.select(limitValue).from(table).where(predicate).fetchOne()

    if (record != null) {
      Some(record.getValueAsInteger(limitValue))
    }
    else {
      None
    }
  })

  private def cascadeLimit(currentLimit: Int, setLimitValue: (ServiceLimit, Int) => Unit,
                           limit: ServiceLimit, newLimit: Int) {
    if (currentLimit > newLimit) {
      setLimitValue(limit, newLimit)
    }
  }

  private def cascadeLimitToSystemDefault(limit: ServiceLimit, limitValue: Int) {
    cascadeLimit(
      getEffectiveLimitByName(limit),
      setSystemDefaultLimit,
      limit, limitValue)
  }
  
  private def cascadeLimitToDomainHardLimit(limit: ServiceLimit, limitValue: Int) {
    val limits = db.listQuery[DomainServiceLimits]("domainServiceLimitsByName", Map("limit_name" -> limit.key))
    limits.foreach { domainLimit =>
      cascadeLimit(
        domainLimit.hardLimit,
        (limitName, limitValue) => setDomainHardLimit(domainLimit.domain.name, limit, limitValue),
        limit, limitValue
      )
    }
  }

  private def cascadeLimitToDomainDefaultLimit(limit: ServiceLimit, limitValue: Int) {
    val limits = db.listQuery[DomainServiceLimits]("domainServiceLimitsByName", Map("limit_name" -> limit.key))
    limits.foreach { domainLimit =>
      cascadeLimit(
        domainLimit.defaultLimit,
        (limitName, limitValue) => setDomainDefaultLimit(domainLimit.domain.name, limit, limitValue),
        limit, limitValue
      )
    }
  }
  
  private def cascadeLimitToPair(limit: ServiceLimit, limitValue: Int) {
    val limits = db.listQuery[PairServiceLimits]("pairServiceLimitsByName", Map("limit_name" -> limit.key))
    limits.foreach { pairLimit =>
      cascadeLimit(
        pairLimit.limitValue,
        (limitName, limitValue) => setPairLimit(pairLimit.pair.domain.name, pairLimit.pair.key, limit, limitValue),
        limit, limitValue
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

  private def setPairLimit(t: Factory, domainName: String, pairKey: String, limit: ServiceLimit, limitValue: Int) = {
    t.insertInto(PAIR_LIMITS).
        set(PAIR_LIMITS.NAME, limit.key).
        set(PAIR_LIMITS.LIMIT_VALUE, int2Integer(limitValue)).
        set(PAIR_LIMITS.DOMAIN, domainName).
        set(PAIR_LIMITS.PAIR_KEY, pairKey).
      onDuplicateKeyUpdate().
        set(PAIR_LIMITS.LIMIT_VALUE, int2Integer(limitValue)).
      execute()
  }

  private def setSystemLimit(limit: ServiceLimit, limitValue: Int, hardOrDefault:TableField[_,_]) = jooq.execute(t => {
    t.insertInto(SYSTEM_LIMITS).
        set(SYSTEM_LIMITS.NAME, limit.key).
        set(hardOrDefault, limitValue).
      onDuplicateKeyUpdate().
        set(hardOrDefault, limitValue).
      execute()
  })

  private def setDomainLimit(domain:String, limit: ServiceLimit, limitValue: Int, hardOrDefault:TableField[_,_]) = jooq.execute(t => {
    t.insertInto(DOMAIN_LIMITS).
        set(DOMAIN_LIMITS.DOMAIN, domain).
        set(DOMAIN_LIMITS.NAME, limit.key).
        set(hardOrDefault, limitValue).
      onDuplicateKeyUpdate().
        set(hardOrDefault, limitValue).
      execute()
  })

  private def deletePairLimitsByDomainInternal(t:Factory, domain: String) = {
    t.delete(PAIR_LIMITS).
      where(PAIR_LIMITS.DOMAIN.equal(domain)).
      execute()
  }
}
