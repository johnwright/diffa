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
import org.slf4j.{Logger, LoggerFactory}
import javax.ws.rs._
import net.lshift.diffa.kernel.config._
import net.lshift.diffa.kernel.frontend.Configuration
import net.lshift.diffa.docgen.annotations.{MandatoryParams, Description}
import net.lshift.diffa.docgen.annotations.MandatoryParams.MandatoryParam

/**
 * This is a REST interface the Configuration abstraction.
 * @see Configuration
 */
@Path("/config")
@Component
class ConfigurationResource extends AbstractRestResource {

  private val log:Logger = LoggerFactory.getLogger(getClass)

  @Autowired var config:Configuration = null

  @GET
  @Path("/endpoints")
  @Produces(Array("application/json"))
  @Description("Returns a list of all the endpoints registered with the agent.")
  def listEndpoints() = config.listEndpoints.toArray

  @GET
  @Path("/groups")
  @Produces(Array("application/json"))
  @Description("Returns a list of all the endpoint groups registered with the agent.")
  def listGroups: Array[GroupContainer] = config.listGroups.toArray

  @GET
  @Produces(Array("application/json"))
  @Path("/endpoints/{id}")
  @Description("Returns an endpoint by its identifier.")
  @MandatoryParams(Array(new MandatoryParam(name="id", datatype="string", description="Endpoint ID")))
  def getEndpoint(@PathParam("id") id:String) = maybe[Endpoint]( x => config.getEndpoint(x), id )

  @POST
  @Path("/endpoints")
  @Consumes(Array("application/json"))
  @Description("Registers a new endpoint with the agent.")
  def createEndpoint(e:Endpoint) = create[Endpoint] (e, (x:Endpoint) => config.createOrUpdateEndpoint(x), (x:Endpoint) => x.name )

  @PUT
  @Consumes(Array("application/json"))
  @Produces(Array("application/json"))
  @Path("/endpoints/{id}")
  @Description("Updates the attributes of an endpoint that is registered with the agent.")
  @MandatoryParams(Array(new MandatoryParam(name="id", datatype="string", description="Endpoint ID")))
  def updateEndpoint(@PathParam("id") id:String, e:Endpoint) = maybeReturn[Endpoint]( e, x => config.createOrUpdateEndpoint(x) )

  @DELETE
  @Path("/endpoints/{id}")
  @Description("Removes an endpoint that is registered with the agent.")
  @MandatoryParams(Array(new MandatoryParam(name="id", datatype="string", description="Endpoint ID")))
  def deleteEndpoint(@PathParam("id") id:String) = maybe[Unit]( x => config.deleteEndpoint(x), id )

  @POST
  @Path("/pairs")
  @Consumes(Array("application/json"))
  @Description("Creates a new pairing between two endpoints that are already registered with the agent.")
  def createPair(p:PairDef) = create[PairDef] (p, (x:PairDef) => config.createOrUpdatePair(x), (x:PairDef) => x.pairKey )

  @PUT
  @Consumes(Array("application/json"))
  @Produces(Array("application/json"))
  @Path("/pairs/{id}")
  @Description("Updates the attributes of a pairing between two endpoints that are already registered with the agent.")
  @MandatoryParams(Array(new MandatoryParam(name="id", datatype="string", description="Pair ID")))
  def updatePair(@PathParam("id") id:String, p:PairDef) = maybeReturn[PairDef]( p, x => config.createOrUpdatePair(x) )

  @DELETE
  @Path("/pairs/{id}")
  @Description("Removes a pairing between two endpoints that are registered with the agent.")
  @MandatoryParams(Array(new MandatoryParam(name="id", datatype="string", description="Pair ID")))
  def deletePair(@PathParam("id") id:String) = maybe[Unit]( x => config.deletePair(x), id )

  @POST
  @Path("/groups")
  @Consumes(Array("application/json"))
  @Description("Creates a new group that can be used to aggregate pairings of endpoints.")
  def createGroup(g:PairGroup) = create[PairGroup] (g, (x:PairGroup) => config.createOrUpdateGroup(x), (x:PairGroup) => x.key )

  @PUT
  @Consumes(Array("application/json"))
  @Produces(Array("application/json"))
  @Path("/groups/{id}")
  @Description("Updates the attributes of a group of endpoint pairings.")
  @MandatoryParams(Array(new MandatoryParam(name="id", datatype="string", description="Group ID")))
  def updateGroup(@PathParam("id") id:String, g:PairGroup) = maybeReturn[PairGroup]( g, x => config.createOrUpdateGroup(x) )

  @DELETE
  @Path("/groups/{id}")
  @Description("Removes a group of endpoint pairings.")
  @MandatoryParams(Array(new MandatoryParam(name="id", datatype="string", description="Group ID")))
  def deleteGroup(@PathParam("id") id:String) = maybe[Unit]( x => config.deleteGroup(x), id )

  @GET
  @Produces(Array("application/json"))
  @Path("/pairs/{id}")
  @Description("Returns an endpoint pairing by its identifier.")
  @MandatoryParams(Array(new MandatoryParam(name="id", datatype="string", description="Pair ID")))
  def getPair(@PathParam("id") id:String) = maybe[Pair]( x => config.getPair(x),id )

  @GET
  @Produces(Array("application/json"))
  @Path("/groups/{id}")
  @Description("Returns a group of endpoint pairings by its identifier.")
  @MandatoryParams(Array(new MandatoryParam(name="id", datatype="string", description="Group ID")))
  def getGroup(@PathParam("id") id:String) = maybe[PairGroup]( x => config.getGroup(x), id )

}
