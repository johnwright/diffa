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
import net.lshift.diffa.participant.scanning.{SetConstraint, ScanConstraint}
import net.lshift.diffa.kernel.frontend.{EscalationDef, RepairActionDef, EndpointDef, PairDef}

/**
 * Provides general configuration options within the scope of a particular domain.
 */
trait DomainConfigStore {

  def createOrUpdateEndpoint(domain:String, endpoint: EndpointDef) : Endpoint
  def deleteEndpoint(domain:String, name: String) : Unit
  def listEndpoints(domain:String) : Seq[EndpointDef]

  def createOrUpdatePair(domain:String, pairDef: PairDef) : Unit
  def deletePair(domain:String, key: String) : Unit
  def listPairs(domain:String) : Seq[PairDef]

  def createOrUpdateRepairAction(domain:String, action: RepairActionDef) : Unit
  def deleteRepairAction(domain:String, name: String, pairKey: String) : Unit

  def listRepairActions(domain:String) : Seq[RepairActionDef]
  def listRepairActionsForPair(domain:String, key: String) : Seq[RepairActionDef]

  def listEscalations(domain:String) : Seq[EscalationDef]
  def deleteEscalation(domain:String, s: String, s1: String)
  def createOrUpdateEscalation(domain:String, escalation : EscalationDef)
  def listEscalationsForPair(domain:String, key: String) : Seq[EscalationDef]

  def getEndpointDef(domain:String, name: String) : EndpointDef
  def getPairDef(domain:String, key: String) : PairDef

  def getRepairActionDef(domain:String, name: String, pairKey: String): RepairActionDef

  /**
   * Retrieves all (domain-specific, non-internal) agent configuration options.
   */
  def allConfigOptions(domain:String) : Map[String, String]

  /**
   * Retrieves an agent configuration option, returning the None if it is unset.
   */
  def maybeConfigOption(domain:String, key:String) : Option[String]

  /**
   * Retrieves an agent configuration option, returning the provided default value if it is unset.
   */
  def configOptionOrDefault(domain:String, key:String, defaultVal:String) : String

  /**
   * Sets the given configuration option to the given value.
   * @param isInternal options marked as internal will not be returned by the allConfigOptions method. This allows
   *   properties to be prevented from being shown in the user-visible system configuration views.
   */
  def setConfigOption(domain:String, key:String, value:String)

  /**
   * Removes the setting for the given configuration option.
   */
  def clearConfigOption(domain:String, key:String)


  /**
   * Make the given user a member of this domain.
   */
  def makeDomainMember(domain:String, userName:String) : Member

  /**
   * Remove the given user a from this domain.
   */
  def removeDomainMembership(domain:String, userName:String) : Unit

  /**
   * Lists all of the members of the given domain
   */
  def listDomainMembers(domain:String) : Seq[Member]
}

case class Endpoint(
  @BeanProperty var name: String = null,
  @BeanProperty var domain: Domain = null,
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
}

case class Pair(
  @BeanProperty var key: String = null,
  @BeanProperty var domain: Domain = null,
  @BeanProperty var upstream: Endpoint = null,
  @BeanProperty var downstream: Endpoint = null,
  @BeanProperty var versionPolicyName: String = null,
  @BeanProperty var matchingTimeout: Int = Pair.NO_MATCHING,
  @BeanProperty var scanCronSpec: String = null) {

  def this() = this(key = null)

  def identifier = asRef.identifier

  def asRef = DiffaPairRef(key,domain.name)

  override def equals(that:Any) = that match {
    case p:Pair => p.key == key && p.domain == domain
    case _      => false
  }

  // TODO This looks a bit strange
  override def hashCode = 31 * (31 + key.hashCode) + domain.hashCode
}

/**
 * This is a light weight pointer to a pair in Diffa.
 */
case class DiffaPairRef(@BeanProperty var key: String = null,
                        @BeanProperty var domain: String = null) {
  def this() = this(key = null)

  def identifier = "%s/%s".format(domain,key)

  def toInternalFormat = Pair(key = key, domain = Domain(name = domain))

}

object Pair {
  val NO_MATCHING = null.asInstanceOf[Int]
  def fromIdentifier(id:String) = {
    val Array(domain,key) = id.split("/")
    (domain,key)
  }
}

case class Domain (
  @BeanProperty var name: String = null
) {
  def this() = this(name = null)

  override def equals(that:Any) = that match {
    case d:Domain => d.name == name
    case _        => false
  }

  override def hashCode = name.hashCode
  override def toString = name
}

object Domain {
  val DEFAULT_DOMAIN = Domain(name = "root")
}


case class RepairAction(
  @BeanProperty var name: String = null,
  @BeanProperty var url: String = null,
  @BeanProperty var scope: String = null,
  @BeanProperty var pair: Pair = null
) {
  import RepairAction._

  def this() = this(name = null)

  def validate(path:String = null) {
    val actionPath = ValidationUtil.buildPath(
      ValidationUtil.buildPath(path, "pair", Map("key" -> pair.key)),
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
  @BeanProperty var name: String = null,
  @BeanProperty var pair: Pair = null,
  @BeanProperty var action: String = null,
  @BeanProperty var actionType: String = null,
  @BeanProperty var event: String = null,
  @BeanProperty var origin: String = null
) {

  def this() = this(name = null)
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

case class User(@BeanProperty var name: String = null,
                @BeanProperty var email: String = null) {
  def this() = this(name = null)

  override def equals(that:Any) = that match {
    case u:User => u.name == name
    case _      => false
  }

  override def hashCode = name.hashCode
  override def toString = name
}

/**
 * Defines a user's membership to a domain
 */
case class Member(@BeanProperty var user: User = null,
                  @BeanProperty var domain: Domain = null) {

  def this() = this(user = null)

}

case class ConfigOption(@BeanProperty var key:String = null,
                        @BeanProperty var value:String = null,
                        @BeanProperty var domain:Domain = null) {
  def this() = this(key = null)
}

/**
 * Convenience wrapper for a compound primary key
 */
case class DomainScopedKey(@BeanProperty var key:String = null,
                           @BeanProperty var domain:Domain = null) extends java.io.Serializable
{
  def this() = this(key = null)
}
