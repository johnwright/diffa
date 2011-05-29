/**
 * Copyright (C) 2010-2011 LShift Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.lshift.diffa.kernel.config

import reflect.BeanProperty
import net.lshift.diffa.kernel.participants.EasyConstraints._
import scala.collection.JavaConversions._
import net.lshift.diffa.kernel.participants.IntegerCategoryFunction._
import java.util.HashMap
import net.lshift.diffa.kernel.differencing.AttributesUtil
import net.lshift.diffa.kernel.participants._

trait ConfigStore {
  def createOrUpdateEndpoint(endpoint: Endpoint): Unit
  def deleteEndpoint(name: String): Unit
  def listEndpoints: Seq[Endpoint]

  def createOrUpdatePair(pairDef: PairDef): Unit
  def deletePair(key: String): Unit

  def createOrUpdateRepairAction(action: RepairActionDef): Unit
  def deleteRepairAction(key: String): Unit

  def createOrUpdateGroup(group: PairGroup): Unit
  def deleteGroup(key: String): Unit
  def getPairsInGroup(group: PairGroup): Seq[Pair]
  def listGroups: Seq[GroupContainer]
  def getRepairActionsForPair(pair: Pair): Seq[Actionable]

  def getEndpoint(name: String): Endpoint
  def getPair(key: String): Pair
  def getGroup(key: String): PairGroup
  def getUser(name: String) : User

  def createOrUpdateUser(user: User): Unit
  def deleteUser(name: String): Unit
  def listUsers: Seq[User]

  def getPairsForEndpoint(epName:String):Seq[Pair]

  /**
   * Retrieves all (non-internal) agent configuration options.
   */
  def allConfigOptions:Map[String, String]

  /**
   * Retrieves an agent configuration option, returning the None if it is unset.
   */
  def maybeConfigOption(key:String):Option[String]

  /**
   * Retrieves an agent configuration option, returning the provided default value if it is unset.
   */
  def configOptionOrDefault(key:String, defaultVal:String):String

  /**
   * Sets the given configuration option to the given value.
   * @param isInternal options marked as internal will not be returned by the allConfigOptions method. This allows
   *   properties to be prevented from being shown in the user-visible system configuration views.
   */
  def setConfigOption(key:String, value:String, isInternal:Boolean = false)

  /**
   * Removes the setting for the given configuration option.
   */
  def clearConfigOption(key:String)
}

case class Endpoint(
  @BeanProperty var name: String = null,
  @BeanProperty var url: String = null,
  @BeanProperty var contentType: String = null,
  @BeanProperty var inboundUrl: String = null,
  @BeanProperty var inboundContentType: String = null,
  @BeanProperty var online: Boolean = false,
  @BeanProperty var categories: java.util.Map[String,CategoryDescriptor] = new HashMap[String, CategoryDescriptor]) {

  def this() = this(null, null, null, null, null, false, new HashMap[String, CategoryDescriptor])

  /**
   * Fuses a list of runtime attributes together with their
   * static schema bound keys because the static attributes
   * are not transmitted over the wire.
   */
  def schematize(runtimeValues:Seq[String]) = AttributesUtil.toTypedMap(categories.toMap, runtimeValues)

  def defaultBucketing() : Map[String, CategoryFunction] = {
    categories.map {
      case (name, categoryType) => {
        categoryType match {
          case s:SetCategoryDescriptor    => name -> ByNameCategoryFunction
          case r:RangeCategoryDescriptor  => name -> RangeTypeRegistry.defaultCategoryFunction(r.dataType)
          case p:PrefixCategoryDescriptor => name -> StringPrefixCategoryFunction(p.prefixLength, p.maxLength, p.step)
        }
      }
    }.toMap
  }

  /**
   * Returns a structured group of constraints for the current endpoint that is appropriate for transmission
   * over the wire.
   */
  def groupedConstraints() : Seq[Seq[QueryConstraint]] = {
    val constraints = defaultConstraints.map(_.group)
    constraints.map(_.map(Seq(_))).reduceLeft((acc, nextConstraints) => for {a <- acc; c <- nextConstraints} yield a ++ c)
  }

  /**
   * Returns a set of the coarsest unbound query constraints for
   * each of the category types that has been configured for this pair.
   */
  def defaultConstraints() : Seq[QueryConstraint] =
    categories.flatMap({
      case (name, categoryType) => {
        categoryType match {
          case s:SetCategoryDescriptor   => Some(SetQueryConstraint(name, s.values.toSet))
          case r:RangeCategoryDescriptor => {
            if (r.lower == null && r.upper == null) {
              Some(RangeTypeRegistry.unboundedConstraint(r.dataType, name))
            }
            else {
              Some(RangeCategoryParser.buildConstraint(name,r))
            }
          }
          case p:PrefixCategoryDescriptor => Some(UnboundedRangeQueryConstraint(name))
        }
      }
    }).toList
}

case class Pair(
  @BeanProperty var key: String = null,
  @BeanProperty var upstream: Endpoint = null,
  @BeanProperty var downstream: Endpoint = null,
  @BeanProperty var group: PairGroup = null,
  @BeanProperty var versionPolicyName: String = null,
  @BeanProperty var matchingTimeout: Int = Pair.NO_MATCHING) {

  def this() = this(null, null, null, null, null, Pair.NO_MATCHING)
}

object Pair {
  val NO_MATCHING = null.asInstanceOf[Int]
}

case class PairGroup(@BeanProperty var key: String) {
  def this() = this(null)
}

case class GroupContainer(@BeanProperty group: PairGroup, @BeanProperty pairs: Array[Pair])

case class PairDef(
  @BeanProperty var pairKey: String,
  @BeanProperty var versionPolicyName: String,
  @BeanProperty var matchingTimeout: Int,
  @BeanProperty var upstreamName: String,
  @BeanProperty var downstreamName: String,
  @BeanProperty var groupKey: String) {

  def this() = this(null, null, null.asInstanceOf[Int], null, null, null)
}

case class RepairActionDef(
  @BeanProperty var key: String = null,
  @BeanProperty var name: String = null,
  @BeanProperty var scope: String = null,
  @BeanProperty var path: String = null,
  @BeanProperty var pairKey: String = null)

case class Actionable (
  @BeanProperty var key:String,
  @BeanProperty var name:String,
  @BeanProperty var scope:String,
  @BeanProperty var path:String,
  @BeanProperty var pairKey:String) {

 def this() = this(null, null, null, null, null)
}

case class User(@BeanProperty var name: String,
                @BeanProperty var email: String) {
  def this() = this(null, null)
}

case class ConfigOption(@BeanProperty var key:String,
                        @BeanProperty var value:String,
                        @BeanProperty var isInternal:Boolean) {
  def this() = this(null, null, false)
}
