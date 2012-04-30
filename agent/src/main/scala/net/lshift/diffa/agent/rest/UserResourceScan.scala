package net.lshift.diffa.agent.rest

import net.lshift.diffa.kernel.config.system.SystemConfigStore
import net.lshift.diffa.kernel.participants.CategoryFunction
import net.lshift.diffa.participant.scanning.{ScanResultEntry, ScanConstraint}
import net.lshift.diffa.kernel.config.User
import scala.collection.JavaConversions._


class UserResourceScan (systemConfigStore: SystemConfigStore)  {
  // def perform() = {};

  def perform(constraints:Seq[ScanConstraint], aggregation:Seq[CategoryFunction]): Seq[ScanResultEntry] = {
    def generateVersion(user:User) = ScannableUtils.generateDigest(user.name, user.token)
         var users = systemConfigStore.listUsers

         var scanResults = users.map  { u =>
           ScanResultEntry.forEntity(u.name, generateVersion(u), null, Map("name" -> u.name))
         }
         ScannableUtils.maybeAggregate(scanResults, aggregation)
  }

}
