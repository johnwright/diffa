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

package net.lshift.diffa.kernel.client

import reflect.BeanProperty
import net.lshift.diffa.kernel.frontend.wire.InvocationResult


/**
 * Interface supported by clients capable of listing and invoking actions for pairs.
 */
trait ActionsClient {

  /**
   * Lists the actions that a pairing offers
   */
  def listActions(pairKey: String) : Seq[Actionable]

  /**
   * Invokes an action against a pairing
   */
  def invoke(request:ActionableRequest) : InvocationResult
  
}

case class ActionableRequest (
  @BeanProperty var pairKey:String,
  @BeanProperty var actionId:String,
  @BeanProperty var entityId:String) {

 def this() = this(null, null, null)

}

case class Actionable (
  @BeanProperty var id:String,
  @BeanProperty var name:String,
  @BeanProperty var `type`:String,
  @BeanProperty var action:String,
  @BeanProperty var method:String) {

 def this() = this(null, null, null, null, null)
 def this(id:String, n:String, t:String, a:String) = this(id, n, t, a, null)

}