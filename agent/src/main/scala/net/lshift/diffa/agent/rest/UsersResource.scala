/**
 * Copyright (C) 2010 LShift Ltd.
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

import org.springframework.stereotype.Component
import org.springframework.beans.factory.annotation.Autowired
import net.lshift.diffa.kernel.frontend.Configuration
import net.lshift.diffa.docgen.annotations.{MandatoryParams, Description}
import net.lshift.diffa.docgen.annotations.MandatoryParams.MandatoryParam
import net.lshift.diffa.kernel.config.User
import javax.ws.rs._

/**
 * This handles all of the user specific admin
 */
@Path("/security")
@Component
class UsersResource extends AbstractRestResource {

  @Autowired var config:Configuration = null

  @GET
  @Path("/users")
  @Produces(Array("application/json"))
  @Description("Returns a list of all the users registered with the agent.")
  def listUsers() = config.listUsers.toArray

  @GET
  @Produces(Array("application/json"))
  @Path("/users/{name}")
  @Description("Returns a user by its name.")
  @MandatoryParams(Array(new MandatoryParam(name="name", datatype="string", description="Username")))
  def getUser(@PathParam("name") name:String) = maybe[User]( x => config.getUser(x), name )

  @POST
  @Path("/users")
  @Consumes(Array("application/json"))
  @Description("Registers a new user with the agent.")
  def createEndpoint(e:User) = create[User] (e, (x:User) => config.createOrUpdateUser(x), (x:User) => x.name )

  @PUT
  @Consumes(Array("application/json"))
  @Produces(Array("application/json"))
  @Path("/users/{name}")
  @Description("Updates the attributes of a user that is registered with the agent.")
  @MandatoryParams(Array(new MandatoryParam(name="name", datatype="string", description="Username")))
  def updateEndpoint(@PathParam("name") name:String, u:User) = maybeReturn[User]( u, x => config.createOrUpdateUser(u) )
  // TODO This PUT is buggy

  @DELETE
  @Path("/users/{name}")
  @Description("Removes an endpoint that is registered with the agent.")
  @MandatoryParams(Array(new MandatoryParam(name="name", datatype="string", description="Username")))
  def deleteEndpoint(@PathParam("name") name:String) = maybe[Unit]( x => config.deleteUser(x), name )
}