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

import javax.ws.rs.{PathParam, Path, DELETE}
import org.springframework.beans.factory.annotation.Autowired
import net.lshift.diffa.kernel.actors.PairPolicyClient
import org.springframework.stereotype.Component
import net.lshift.diffa.docgen.annotations.{MandatoryParams, Description}
import net.lshift.diffa.docgen.annotations.MandatoryParams.MandatoryParam

@Path("/scanning")
@Component
class ScanningResource {

  @Autowired var pairPolicyClient:PairPolicyClient = null

  @DELETE
  @Path("/pairs/{pairKey}/scan")
  @Description("Cancels any current and/or pending scans for the given pair.")
  @MandatoryParams(Array(new MandatoryParam(name="pairKey", datatype="string", description="Pair Key")))
  def cancelScanning(@PathParam("pairKey") pairKey:String) = pairPolicyClient.cancelScans(pairKey)

}