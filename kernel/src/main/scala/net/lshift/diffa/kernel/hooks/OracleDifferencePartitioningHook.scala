package net.lshift.diffa.kernel.hooks

import org.hibernate.SessionFactory
import net.lshift.diffa.kernel.util.SessionHelper._
import org.apache.commons.lang.StringEscapeUtils
import org.apache.commons.codec.digest.DigestUtils

/**
 * Oracle implementation of the DifferencePartitioningHook. Assumes that the underlying table has been list-partitioned
 * using a virtual column combining domain and pair key with an underscore (eg, 'diffa_pair1).
 */
class OracleDifferencePartitioningHook(sessionFactory:SessionFactory) extends DifferencePartitioningHook {
  def pairCreated(domain: String, key: String) {
    // Create the relevant partition
    sessionFactory.withSession(s => {
      val escapedVal = StringEscapeUtils.escapeSql(domain + "_" + key)
      val partitionName = generatePartitionName(domain, key)

      val query = s.createSQLQuery("alter table diffs add partition " + partitionName + " values('" + escapedVal + "')");
      query.executeUpdate()
    })
  }

  def pairRemoved(domain: String, key: String) {
    // Drop the relevant partition on the diffs table
    sessionFactory.withSession(s => {
      val partitionName = generatePartitionName(domain, key)
      val query = s.createSQLQuery("alter table diffs drop partition " + partitionName)
      query.executeUpdate()
    })
  }

  def removeAllPairDifferences(domain: String, key: String) = {
    sessionFactory.withSession(s => {
      val partitionName = generatePartitionName(domain, key)
      val query = s.createSQLQuery("alter table diffs truncate partition " + partitionName)
      query.executeUpdate()
      true
    })
  }

  /**
   * Generates a unique, repeatable partition name based on using the domain and key, preventing SQL-unsafe characters
   * from causing any kind of injection attack.
   */
  def generatePartitionName(domain:String, key:String) = "p_" + DigestUtils.md5Hex(domain + "_" + key).substring(0, 28)
}