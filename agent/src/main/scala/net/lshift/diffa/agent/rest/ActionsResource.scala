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
import org.springframework.stereotype.Component
import org.springframework.beans.factory.annotation.Autowired
import net.lshift.diffa.docgen.annotations.{MandatoryParams, Description}
import net.lshift.diffa.docgen.annotations.MandatoryParams.MandatoryParam
import net.lshift.diffa.kernel.frontend.wire.InvocationResult
import net.lshift.diffa.kernel.client.{Actionable, ActionableRequest, ActionsClient}

@Path("/actions")
@Component
class ActionsResource extends AbstractRestResource {

  @Autowired var proxy:ActionsClient = null

  @GET
  @Path("/{pairId}")
  @Produces(Array("application/json"))
  @Description("Returns a list of actions that can be invoked on the pair. ")
  @MandatoryParams(Array(new MandatoryParam(name="pairId", datatype="string", description="The identifier of the pair")))
  def listActions(@PathParam("pairId") pairId:String) : Array[Actionable] = {
    maybe( (x:String) => proxy.listActions(x).toArray, pairId )
  }

  @POST
  @Path("/{pairId}/{actionId}/{entityId}")
  @Produces(Array("application/json"))  
  @Description("Invokes a named action on the pair and passes the id of the entity to which the action should be applied. ")
  @MandatoryParams(Array(
    new MandatoryParam(name="pairId", datatype="string", description="The indentifier of the pair"),
    new MandatoryParam(name="actionId", datatype="string", description="The name of the action to invoke"),
    new MandatoryParam(name="entityId", datatype="string", description="The id of the pair to perform the action on")
  ))
  def invokeAction(@PathParam("pairId") pairId:String,
                   @PathParam("actionId") actionId:String,
                   @PathParam("entityId") entityId:String) : InvocationResult = {
    maybe( (x:String) => proxy.invoke(ActionableRequest(x, actionId, entityId)), pairId )
  }

}