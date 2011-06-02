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

import org.springframework.stereotype.Component
import org.springframework.beans.factory.annotation.Autowired
import org.slf4j.{Logger, LoggerFactory}
import javax.ws.rs._
import core.{UriInfo, Context}
import net.lshift.diffa.kernel.config._
import net.lshift.diffa.docgen.annotations.{MandatoryParams, Description}
import net.lshift.diffa.docgen.annotations.MandatoryParams.MandatoryParam
import net.lshift.diffa.kernel.frontend.{DiffaConfig, Configuration}
import scala.collection.JavaConversions._
import net.lshift.diffa.agent.rest.ResponseUtils._

/**
 * This is a REST interface the Configuration abstraction.
 * @see Configuration
 */
@Path("/config")
@Component
class ConfigurationResource {

  @Autowired var config:Configuration = null
  @Context var uriInfo:UriInfo = null

  @GET
  @Path("/xml")
  @Produces(Array("application/xml"))
  def retrieveConfiguration():DiffaConfig = config.retrieveConfiguration

  @POST
  @Path("/xml")
  @Consumes(Array("application/xml"))
  def applyConfiguration(newConfig:DiffaConfig) = config.applyConfiguration(newConfig)

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
  @Path("/repair-actions")
  @Produces(Array("application/json"))
  @Description("Returns a list of all the repair actions registered with the agent.")
  def listRepairActions: Array[RepairAction] = config.listRepairActions.toArray

  @GET
  @Produces(Array("application/json"))
  @Path("/endpoints/{id}")
  @Description("Returns an endpoint by its identifier.")
  @MandatoryParams(Array(new MandatoryParam(name="id", datatype="string", description="Endpoint ID")))
  def getEndpoint(@PathParam("id") id:String) = config.getEndpoint(id)

  @POST
  @Path("/endpoints")
  @Consumes(Array("application/json"))
  @Description("Registers a new endpoint with the agent.")
  def createEndpoint(e:Endpoint) = {
    config.createOrUpdateEndpoint(e)
    resourceCreated(e.name, uriInfo)
  }

  @PUT
  @Consumes(Array("application/json"))
  @Produces(Array("application/json"))
  @Path("/endpoints/{id}")
  @Description("Updates the attributes of an endpoint that is registered with the agent.")
  @MandatoryParams(Array(new MandatoryParam(name="id", datatype="string", description="Endpoint ID")))
  def updateEndpoint(@PathParam("id") id:String, e:Endpoint) = {
    config.createOrUpdateEndpoint(e)
    e
  }

  @DELETE
  @Path("/endpoints/{id}")
  @Description("Removes an endpoint that is registered with the agent.")
  @MandatoryParams(Array(new MandatoryParam(name="id", datatype="string", description="Endpoint ID")))
  def deleteEndpoint(@PathParam("id") id:String) = config.deleteEndpoint(id)

  @POST
  @Path("/pairs")
  @Consumes(Array("application/json"))
  @Description("Creates a new pairing between two endpoints that are already registered with the agent.")
  def createPair(p:PairDef) = {
    config.createOrUpdatePair(p)
    resourceCreated(p.pairKey, uriInfo)
  }

  @PUT
  @Consumes(Array("application/json"))
  @Produces(Array("application/json"))
  @Path("/pairs/{id}")
  @Description("Updates the attributes of a pairing between two endpoints that are already registered with the agent.")
  @MandatoryParams(Array(new MandatoryParam(name="id", datatype="string", description="Pair ID")))
  def updatePair(@PathParam("id") id:String, p:PairDef) = {
    config.createOrUpdatePair(p)
    p
  }

  @DELETE
  @Path("/pairs/{id}")
  @Description("Removes a pairing between two endpoints that are registered with the agent.")
  @MandatoryParams(Array(new MandatoryParam(name="id", datatype="string", description="Pair ID")))
  def deletePair(@PathParam("id") id:String) = config.deletePair(id)

  @GET
  @Path("/pairs/{id}/repair-actions")
  @Produces(Array("application/json"))
  @Description("Returns a list of the repair actions associated with a pair")
  @MandatoryParams(Array(new MandatoryParam(name="id", datatype="string", description="Pair ID")))
  def listRepairActionsForPair(@PathParam("id") pairKey: String) = config.listRepairActionsForPair(pairKey).toArray

  @POST
  @Path("/pairs/{id}/repair-actions")
  @Consumes(Array("application/json"))
  @MandatoryParams(Array(new MandatoryParam(name="id", datatype="string", description="Pair ID")))
  @Description("Creates a new repair action associated with a pair that is registered with the agent.")
  def createRepairAction(a: RepairAction) = {
    config.createOrUpdateRepairAction(a)
    resourceCreated(a.name, uriInfo)
  }

  @DELETE
  @Path("/pairs/{pairKey}/repair-actions/{name}")
  @Description("Removes an action that is registered with the agent.")
  @MandatoryParams(Array(new MandatoryParam(name="pairKey", datatype="string", description="Pair ID"),
                         new MandatoryParam(name="name", datatype="string", description="Action name")))
  def deleteRepairAction(@PathParam("name") name: String, @PathParam("pairKey") pairKey: String) {
    config.deleteRepairAction(name, pairKey)
  }

  @POST
  @Path("/groups")
  @Consumes(Array("application/json"))
  @Description("Creates a new group that can be used to aggregate pairings of endpoints.")
  def createGroup(g:PairGroup) = {
    config.createOrUpdateGroup(g)
    resourceCreated(g.key, uriInfo)
  }

  @PUT
  @Consumes(Array("application/json"))
  @Produces(Array("application/json"))
  @Path("/groups/{id}")
  @Description("Updates the attributes of a group of endpoint pairings.")
  @MandatoryParams(Array(new MandatoryParam(name="id", datatype="string", description="Group ID")))
  def updateGroup(@PathParam("id") id:String, g:PairGroup) = {
    config.createOrUpdateGroup(g)
    g
  }

  @DELETE
  @Path("/groups/{id}")
  @Description("Removes a group of endpoint pairings.")
  @MandatoryParams(Array(new MandatoryParam(name="id", datatype="string", description="Group ID")))
  def deleteGroup(@PathParam("id") id:String) = config.deleteGroup(id)

  @GET
  @Produces(Array("application/json"))
  @Path("/pairs/{id}")
  @Description("Returns an endpoint pairing by its identifier.")
  @MandatoryParams(Array(new MandatoryParam(name="id", datatype="string", description="Pair ID")))
  def getPair(@PathParam("id") id:String) = config.getPair(id)

  @GET
  @Produces(Array("application/json"))
  @Path("/groups/{id}")
  @Description("Returns a group of endpoint pairings by its identifier.")
  @MandatoryParams(Array(new MandatoryParam(name="id", datatype="string", description="Group ID")))
  def getGroup(@PathParam("id") id:String) = config.getGroup(id)

}
