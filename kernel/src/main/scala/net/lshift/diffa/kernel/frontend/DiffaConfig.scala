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
package net.lshift.diffa.kernel.frontend

import net.lshift.diffa.kernel.config._
import net.lshift.diffa.kernel.config.{Pair => DiffaPair}
import reflect.BeanProperty
import org.quartz.CronExpression
import java.util.HashMap

/**
 * Describes a complete Diffa configuration in the context of a domain - this means that all of the objects
 * defined in a single config belong to a particular domain.
 */
case class DiffaConfig(
  members:Set[String] = Set(),
  properties:Map[String, String] = Map(),
  endpoints:Set[EndpointDef] = Set(),
  pairs:Set[PairDef] = Set(),
  repairActions:Set[RepairActionDef] = Set(),
  escalations:Set[EscalationDef] = Set()
) {

  def validate() {
    val path = "config"
    endpoints.foreach(_.validate(path))
    pairs.foreach(_.validate(path))
    repairActions.foreach(_.validate(path))
    escalations.foreach(_.validate(path))
  }
}

/**
 * Serializable representation of an Endpoint within the context of a domain.
 */
case class EndpointDef (
  @BeanProperty var name: String = null,
  @BeanProperty var scanUrl: String = null,
  @BeanProperty var contentRetrievalUrl: String = null,
  @BeanProperty var versionGenerationUrl: String = null,
  @BeanProperty var contentType: String = null,
  @BeanProperty var inboundUrl: String = null,
  @BeanProperty var inboundContentType: String = null,
  @BeanProperty var categories: java.util.Map[String,CategoryDescriptor] = new HashMap[String, CategoryDescriptor],
  @BeanProperty var views: java.util.List[EndpointView] = new java.util.ArrayList[EndpointView]) {

  def this() = this(name = null)

  val DEFAULT_URL_LENGTH_LIMIT = 1024

  def validate(path:String = null) {
    // TODO [#344] : Add validation of endpoint parameters
    val endPointPath = ValidationUtil.buildPath(path, "endpoint", Map("name" -> name))
    if (contentType == null) {
      throw new ConfigValidationException(endPointPath, "Content type should not be null for endpoint %s".format(this))
    }
    Seq(
      scanUrl,
      contentRetrievalUrl,
      versionGenerationUrl,
      inboundUrl
    ).foreach(url => {
      if (url != null) {
        if (url.length() > DEFAULT_URL_LENGTH_LIMIT) {
          throw new ConfigValidationException(endPointPath,
            "URL (%s) length was %s, but the limit is %s (endpoint was %s)".format(url, url.length(), DEFAULT_URL_LENGTH_LIMIT, this))
        }
      }
    })
  }
}

/**
 * Serializable representation of a Pair within the context of a domain.
 */
case class PairDef(
  @BeanProperty var key: String = null,
  @BeanProperty var versionPolicyName: String = null,
  @BeanProperty var matchingTimeout: Int = 0,
  @BeanProperty var upstreamName: String = null,
  @BeanProperty var downstreamName: String = null,
  @BeanProperty var scanCronSpec: String = null,
  @BeanProperty var views:java.util.List[PairView] = new java.util.ArrayList[PairView]) {

  def this() = this(key = null)

  def validate(path:String = null) {
    val pairPath = ValidationUtil.buildPath(path, "pair", Map("key" -> key))

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

/**
 * Serializable representation of a RepairAction within the context of a domain.
 */
case class RepairActionDef (
  @BeanProperty var name: String = null,
  @BeanProperty var url: String = null,
  @BeanProperty var scope: String = null,
  @BeanProperty var pair: String = null
) {
  import RepairAction._

  def this() = this(name = null)

  def validate(path:String = null) {
    val actionPath = ValidationUtil.buildPath(
      ValidationUtil.buildPath(path, "pair", Map("key" -> pair)),
      "repair-action", Map("name" -> name))

    // Ensure that the scope is supported
    this.scope = scope match {
      case ENTITY_SCOPE | PAIR_SCOPE => scope
      case _ => throw new ConfigValidationException(actionPath, "Invalid action scope: "+scope)
    }
  }

  def asRepairAction(domain:String)
    = RepairAction(name, url, scope, DiffaPair(key=pair,domain=Domain(name=domain)))
}

/**
 * Serializable representation of an Escalation within the context of a domain.
 */
case class EscalationDef (
  @BeanProperty var name: String = null,
  @BeanProperty var pair: String = null,
  @BeanProperty var action: String = null,
  @BeanProperty var actionType: String = null,
  @BeanProperty var event: String = null,
  @BeanProperty var origin: String = null
) {
  import EscalationEvent._
  import EscalationOrigin._
  import EscalationActionType._

  def this() = this(name = null)

  def validate(path:String = null) {
    val escalationPath = ValidationUtil.buildPath(
      ValidationUtil.buildPath(path, "pair", Map("key" -> pair)),
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

  def asEscalation(domain:String)
    = Escalation(name, DiffaPair(key=pair,domain=Domain(name=domain)), action, actionType, event, origin)
}

