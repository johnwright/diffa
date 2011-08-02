package net.lshift.diffa.agent.client

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

import net.lshift.diffa.messaging.json.AbstractRestClient
import javax.ws.rs.core.MediaType
import com.sun.jersey.api.client.ClientResponse

/**
 * A RESTful client to manage participant scanning.
 */
class ScanningRestClient(u:String, domain:String) extends AbstractRestClient(u, domain, "scanning/") {

  def startScan(pairKey: String) = {
    val p = resource.path("pairs").path(pairKey).path("scan")
    val response = p.accept(MediaType.APPLICATION_JSON_TYPE).post(classOf[ClientResponse])
    val status = response.getClientResponseStatus
    status.getStatusCode match {
      case 202     => // Successfully submitted (202 is "Accepted")
      case x:Int   => throw new RuntimeException("HTTP " + x + " : " + status.getReasonPhrase)
    }
    true
  }

  def cancelScanning(pairKey: String) = {
    delete("/pairs/" + pairKey + "/scan")
    true
  }
}