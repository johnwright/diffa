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

package net.lshift.diffa.messaging.json

import com.sun.jersey.api.client.config.{ClientConfig, DefaultClientConfig}
import org.codehaus.jackson.jaxrs.JacksonJsonProvider
import javax.ws.rs.core.MediaType
import com.sun.jersey.api.client.{ClientResponse, Client}
import org.slf4j.{LoggerFactory, Logger}
import org.apache.commons.io.IOUtils
import java.io.Closeable

abstract class AbstractRestClient(val serverRootUrl:String, val restResourceSubUrl:String) extends Closeable {

  val log:Logger = LoggerFactory.getLogger(getClass)

  log.debug("Configured to initialize using the server URL (" + serverRootUrl + ") with a sub URL (" + restResourceSubUrl + ")")

  private var isClosing = false

  // TODO Implement proper authentication
  val config = new DefaultClientConfig()
  config.getProperties().put(ClientConfig.PROPERTY_FOLLOW_REDIRECTS, true.asInstanceOf[AnyRef]);
  config.getClasses().add(classOf[JacksonJsonProvider]);
  val client = Client.create(config)
  val serverRootResource = client.resource(serverRootUrl)
  val resource = serverRootResource.path(restResourceSubUrl)

  override def close() = {
    if (!isClosing) {
      isClosing = true
      client.destroy
    }    
  }

  // TODO This is quite sketchy at the moment
  def rpc(path:String) = {
    val response = resource.path(path).get(classOf[String])
    response
  }

  def executeRpc(path:String, body:String) : Option[String] = {
    val submitEndpoint = resource.path(path).`type`(MediaType.APPLICATION_JSON_TYPE)
    val response = submitEndpoint.post(classOf[ClientResponse], body)
    response.getStatus match {
      case 200 => Some(IOUtils.toString(response.getEntityInputStream, "UTF-8"))
      case _   => {
        log.error(response.getStatus + "")
        None
      }
    }

  }

  def submit(path:String, body:String) = {
    val submitEndpoint = resource.path(path).`type`(MediaType.APPLICATION_JSON_TYPE)
    val response = submitEndpoint.post(classOf[ClientResponse], body)
    response
  }

  def rpc[T](path:String, t:Class[T]) = resource.path(path).get(t)

  def create (where:String, what:Any) = {
    val endpoint = resource.path(where)
    val media = endpoint.`type`(MediaType.APPLICATION_JSON_TYPE)
    val response = media.post(classOf[ClientResponse], what)
    response.getStatus match {
      case 201 => ()
      case _   => {
        log.error(response.getStatus + "")
        throw new RuntimeException(response.getStatus() + "")
      }
    }
  }
}