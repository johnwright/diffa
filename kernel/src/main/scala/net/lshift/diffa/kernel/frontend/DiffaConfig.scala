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

/**
 * Describes a complete diffa configuration.
 */
case class DiffaConfig(
  users:Set[User] = Set(),
  properties:Map[String, String] = Map(),
  endpoints:Set[Endpoint] = Set(),
  pairs:Set[PairDef] = Set(),
  repairActions:Set[RepairActionDef] = Set(),
  escalations:Set[EscalationDef] = Set()
) {

  def validate() {
    val path = "config"

    users.foreach(_.validate(path))
    endpoints.foreach(_.validate(path))
    pairs.foreach(_.validate(path))
    repairActions.foreach(_.validate(path))
    escalations.foreach(_.validate(path))
  }
}

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

