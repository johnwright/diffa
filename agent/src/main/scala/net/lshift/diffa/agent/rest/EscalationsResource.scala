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

package net.lshift.diffa.agent.rest

import javax.ws.rs._
import net.lshift.diffa.kernel.frontend.{EscalationDef, Configuration}

/**
 * ATM this resource proxies directly through to the underlying configuration, because the current scope of
 * escalations is quite minimal.
 *
 * This is likely to change when #274 lands
 */
class EscalationsResource(val config:Configuration,
                          val domain:String) {

  @GET
  @Path("/{pairId}")
  @Produces(Array("application/json"))
  def listEscalations(@PathParam("pairId") pairId: String): Array[EscalationDef] = config.listEscalationForPair(domain, pairId).toArray

}