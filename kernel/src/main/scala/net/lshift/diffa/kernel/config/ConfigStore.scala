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
import scala.collection.JavaConversions._
import java.util.HashMap
import net.lshift.diffa.kernel.differencing.AttributesUtil
import net.lshift.diffa.kernel.participants._
import org.quartz.CronExpression
import net.lshift.diffa.participant.scanning.{SetConstraint, ScanConstraint}

trait ConfigStore {
  def createOrUpdateEndpoint(endpoint: Endpoint): Unit
  def deleteEndpoint(name: String): Unit
  def listEndpoints: Seq[Endpoint]

  def createOrUpdatePair(pairDef: PairDef): Unit
  def deletePair(key: String): Unit

  def createOrUpdateRepairAction(action: RepairAction): Unit
  def deleteRepairAction(name: String, pairKey: String): Unit

  def createOrUpdateGroup(group: PairGroup): Unit
  def deleteGroup(key: String): Unit
  def getPairsInGroup(group: PairGroup): Seq[Pair]
  def listGroups: Seq[GroupContainer]

  def createOrUpdateDomain(domain: Domain): Unit

  def listRepairActions: Seq[RepairAction]
  def listRepairActionsForPair(pair: Pair): Seq[RepairAction]

  def listEscalations: Seq[Escalation]
  def deleteEscalation(s: String, s1: String)
  def createOrUpdateEscalation(escalation: Escalation)
  def listEscalationsForPair(pair: Pair): Seq[Escalation]

  def getEndpoint(name: String): Endpoint
  def getPair(key: String): Pair
  def getGroup(key: String): PairGroup
  // TODO Does this get called?
  def getDomain(key: String): Domain
  def getUser(name: String) : User
  def getRepairAction(name: String, pairKey: String): RepairAction

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
  @BeanProperty var scanUrl: String = null,
  @BeanProperty var contentRetrievalUrl: String = null,
  @BeanProperty var versionGenerationUrl: String = null,
  @BeanProperty var contentType: String = null,
  @BeanProperty var inboundUrl: String = null,
  @BeanProperty var inboundContentType: String = null,
  @BeanProperty var categories: java.util.Map[String,CategoryDescriptor] = new HashMap[String, CategoryDescriptor]) {

  def this() = this(name = null)

  /**
   * Fuses a list of runtime attributes together with their
   * static schema bound keys because the static attributes
   * are not transmitted over the wire.
   */
  def schematize(runtimeValues:Seq[String]) = AttributesUtil.toTypedMap(categories.toMap, runtimeValues)

  def defaultBucketing() : Seq[CategoryFunction] = {
    categories.flatMap {
      case (name, categoryType) => {
        categoryType match {
          // #203: By default, set elements should be sent out individually. The default behaviour for an
          // un-aggregated attribute is to handle it by name, so we don't need to return any bucketing for it.
          case s:SetCategoryDescriptor    => None
          case r:RangeCategoryDescriptor  => Some(RangeTypeRegistry.defaultCategoryFunction(name, r.dataType))
          case p:PrefixCategoryDescriptor => Some(StringPrefixCategoryFunction(name, p.prefixLength, p.maxLength, p.step))
        }
      }
    }.toSeq
  }

  /**
   * Returns a structured group of constraints for the current endpoint that is appropriate for transmission
   * over the wire.
   */
  def groupedConstraints() : Seq[Seq[ScanConstraint]] = {
    val constraints = defaultConstraints.map {
      /**
       * #203: By default, set elements should be sent out individually - in the future, this may be configurable
       */
      case sc:SetConstraint =>
        sc.getValues.map(v => new SetConstraint(sc.getAttributeName, Set(v))).toSeq
      case c                =>
        Seq(c)
    }
    if (constraints.length > 0) {
      constraints.map(_.map(Seq(_))).reduceLeft((acc, nextConstraints) => for {a <- acc; c <- nextConstraints} yield a ++ c)
    } else {
      Seq()
    }
  }

  /**
   * Returns a set of the coarsest unbound query constraints for
   * each of the category types that has been configured for this pair.
   */
  def defaultConstraints() : Seq[ScanConstraint] =
    categories.flatMap({
      case (name, categoryType) => {
        categoryType match {
          case s:SetCategoryDescriptor   =>
            Some(new SetConstraint(name, s.values))
          case r:RangeCategoryDescriptor => {
            if (r.lower == null && r.upper == null) {
              None
            }
            else {
              Some(RangeCategoryParser.buildConstraint(name,r))
            }
          }
          case p:PrefixCategoryDescriptor =>
            None
        }
      }
    }).toList

  def validate(path:String = null) {
    // TODO: Add validation of endpoint parameters
  }
}

case class Pair(
  @BeanProperty var key: String = null,
  @BeanProperty var upstream: Endpoint = null,
  @BeanProperty var downstream: Endpoint = null,
  @BeanProperty var group: PairGroup = null,
  @BeanProperty var versionPolicyName: String = null,
  @BeanProperty var matchingTimeout: Int = Pair.NO_MATCHING,
  @BeanProperty var scanCronSpec: String = null) {

  def this() = this(key = null)
}

object Pair {
  val NO_MATCHING = null.asInstanceOf[Int]
}

case class PairGroup(@BeanProperty var key: String) {
  def this() = this(null)

  def validate(path:String = null) {
    // Nothing to validate
  }
}

case class GroupContainer(@BeanProperty group: PairGroup, @BeanProperty pairs: Array[Pair])

case class PairDef(
  @BeanProperty var pairKey: String = null,
  @BeanProperty var versionPolicyName: String = null,
  @BeanProperty var matchingTimeout: Int = 0,
  @BeanProperty var upstreamName: String = null,
  @BeanProperty var downstreamName: String = null,
  @BeanProperty var groupKey: String = null,
  @BeanProperty var scanCronSpec: String = null) {

  def this() = this(pairKey = null)

  def validate(path:String = null) {
    val pairPath = ValidationUtil.buildPath(path, "pair", Map("key" -> pairKey))

    // Ensure that cron specs are valid
    if (scanCronSpec != null) {
      try {
        // Will throw an exception if the expression is invalid. The exception message will also include useful
        // diagnostics of why it is wrong.
        new CronExpression(scanCronSpec)
      } catch {
        case ex =>
          throw new ConfigValidationException(pairPath, "Schedule '" + scanCronSpec + "' is not a valid: " + ex.getMessage)
      }
    }
  }
}

case class Domain (
  @BeanProperty var name: String
) {
  def this() = this(null)

  def validate(path:String = null) {
    // Nothing to validate
  }
}


case class RepairAction(
  @BeanProperty var name: String,
  @BeanProperty var url: String,
  @BeanProperty var scope: String,
  @BeanProperty var pairKey: String
) {
  import RepairAction._

  def this() = this(null, null, null, null)

  def validate(path:String = null) {
    val actionPath = ValidationUtil.buildPath(
      ValidationUtil.buildPath(path, "pair", Map("key" -> pairKey)),
      "repair-action", Map("name" -> name))

    // Ensure that the scope is supported
    this.scope = scope match {
      case ENTITY_SCOPE | PAIR_SCOPE => scope
      case _ => throw new ConfigValidationException(actionPath, "Invalid action scope: "+scope)
    }
  }
}

object RepairAction {
  val ENTITY_SCOPE = "entity"
  val PAIR_SCOPE = "pair"
}
/**
 * Defines a step for escalating a detected difference.
 */
case class Escalation (
  @BeanProperty var name: String,
  @BeanProperty var pairKey: String,
  @BeanProperty var action: String,
  @BeanProperty var actionType: String,
  @BeanProperty var event: String,
  @BeanProperty var origin: String
) {
  import EscalationEvent._
  import EscalationOrigin._
  import EscalationActionType._

  def this() = this(null, null, null, null, null, null)

  def validate(path:String = null) {
    val escalationPath = ValidationUtil.buildPath(
      ValidationUtil.buildPath(path, "pair", Map("key" -> pairKey)),
      "escalation", Map("name" -> name))

    // Ensure that the event is supported
    this.event = event match {
      case UPSTREAM_MISSING | DOWNSTREAM_MISSING | MISMATCH  => event
      case _ => throw new ConfigValidationException(escalationPath, "Invalid escalation event: " + event)
    }
    // Ensure that the origin is supported
    this.origin = origin match {
      case SCAN => origin
      case _    => throw new ConfigValidationException(escalationPath, "Invalid escalation origin: " + origin)
    }
    // Ensure that the action type is supported
    this.actionType = actionType match {
      case REPAIR => actionType
      case _    => throw new ConfigValidationException(escalationPath, "Invalid escalation action type: " + actionType)
    }
  }
}

/**
 * Enumeration of valid types that an escalating difference should trigger.
 */
object EscalationEvent {
  val UPSTREAM_MISSING = "upstream-missing"
  val DOWNSTREAM_MISSING = "downstream-missing"
  val MISMATCH = "mismatch"
}

/**
 * Enumeration of valid origins for escalating a difference.
 */
object EscalationOrigin {
  val SCAN = "scan"
}

/**
 * Enumeration of valid action types for escalating a difference.
 */
object EscalationActionType {
  val REPAIR = "repair"
}

case class User(@BeanProperty var name: String,
                @BeanProperty var email: String) {
  def this() = this(null, null)

  def validate(path:String = null) {
    // Nothing to validate
  }
}

case class ConfigOption(@BeanProperty var key:String,
                        @BeanProperty var value:String,
                        @BeanProperty var isInternal:Boolean) {
  def this() = this(null, null, false)
}
