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
import org.springframework.stereotype.Component
import net.lshift.diffa.agent.rest.ResponseUtils._
import javax.ws.rs.core.{UriInfo, Context}
import net.lshift.diffa.docgen.annotations.{MandatoryParams, Description}
import net.lshift.diffa.docgen.annotations.MandatoryParams.MandatoryParam
import javax.ws.rs._
import net.lshift.diffa.kernel.frontend.{SystemConfiguration, DomainDef}

@Path("/root")
@Component
class SystemConfigResource {

  @Autowired var systemConfig:SystemConfiguration = null
  @Context var uriInfo:UriInfo = null

  @POST
  @Path("/domains")
  @Consumes(Array("application/json"))
  @Description("Creates a new domain within the agent.")
  def createDomain(domain:DomainDef) = {
    systemConfig.createOrUpdateDomain(domain)
    resourceCreated(domain.name, uriInfo)
  }

  @DELETE
  @Path("/domains/{name}")
  @Description("Removes a domain from the agent.")
  @MandatoryParams(Array(new MandatoryParam(name="name", datatype="string", description="Domain name")))
  def deleteEndpoint(@PathParam("name") name:String) = systemConfig.deleteDomain(name)
}