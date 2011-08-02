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

import org.springframework.beans.factory.annotation.Autowired
import net.lshift.diffa.kernel.config.system.SystemConfigStore
import org.springframework.stereotype.Component
import net.lshift.diffa.agent.rest.ResponseUtils._
import net.lshift.diffa.kernel.frontend.DomainDef
import net.lshift.diffa.kernel.frontend.FrontendConversions._
import javax.ws.rs.core.{UriInfo, Context}
import net.lshift.diffa.docgen.annotations.{MandatoryParams, Description}
import net.lshift.diffa.docgen.annotations.MandatoryParams.MandatoryParam
import javax.ws.rs._

@Path("/root")
@Component
class SystemConfigResource {

  @Autowired var systemConfig:SystemConfigStore = null
  @Context var uriInfo:UriInfo = null

  @POST
  @Path("/domains")
  @Consumes(Array("application/json"))
  @Description("Creates a new domain within the agent.")
  def createDomain(domain:DomainDef) = {
    // TODO Should we have a frontend that is analogus to the Configuration frontend, or should we re-use that, or just leave it?
    domain.validate()
    systemConfig.createOrUpdateDomain(fromDomainDef(domain))
    resourceCreated(domain.name, uriInfo)
  }

  @DELETE
  @Path("/domains/{name}")
  @Description("Removes a domain from the agent.")
  @MandatoryParams(Array(new MandatoryParam(name="name", datatype="string", description="Domain name")))
  def deleteEndpoint(@PathParam("name") name:String) = systemConfig.deleteDomain(name)
}