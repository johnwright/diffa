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
package net.lshift.diffa.agent.itest

import org.junit.Test
import support.TestEnvironments
import net.lshift.diffa.agent.util.ConfigComparisonUtil
import org.apache.commons.io.IOUtils
import javax.ws.rs.core.MediaType
import com.sun.jersey.api.client.{Client, ClientResponse}
import com.sun.jersey.api.client.config.{ClientConfig, DefaultClientConfig}
import org.codehaus.jackson.jaxrs.JacksonJsonProvider
/**
 * Tests for bulk configuration upload over the rest interface.
 */
class ConfigurationUploadTest {
  @Test
  def shouldUploadAndDownloadConfig() {
      // Don't create any endpoints or pairs, since they'll be verified on upload and will put the agent into a
      // bad state.
    var config =
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "<diffa-config>" +
        "<property key=\"a\">b</property>" +
        "<user name=\"foo\" email=\"foo@bar.com\"/>" +
        "<group name=\"gaa\">" +
        "</group>" +
        "<group name=\"gbb\">" +
        "</group>" +
      "</diffa-config>"

    uploadConfig(config)
    val retrieved = retrieveConfig

    ConfigComparisonUtil.assertConfigMatches(config, retrieved)
  }

  @Test
  def shouldAllowBlankingOfConfig() {
    val empty = "<diffa-config />"

    uploadConfig(empty)
    val retrieved = retrieveConfig

    ConfigComparisonUtil.assertConfigMatches(empty, retrieved)

  }

  // Create our own Client here, since this mechanism doesn't really fit any of the other REST clients

  val config = new DefaultClientConfig()
  config.getProperties().put(ClientConfig.PROPERTY_FOLLOW_REDIRECTS, true.asInstanceOf[AnyRef]);
  config.getClasses().add(classOf[JacksonJsonProvider]);
  val client = Client.create(config)
  val serverRootResource = client.resource("http://localhost:19093/diffa-agent")
  val resource = serverRootResource.path("rest/config/xml").`type`(MediaType.APPLICATION_XML_TYPE)

  def uploadConfig(body:String) = {
    val response = resource.post(classOf[ClientResponse], body)
    val responseContent = IOUtils.toString(response.getEntityInputStream, "UTF-8")

    response.getStatus match {
      case 204 => responseContent
      case _   => throw new RuntimeException("Unexpected response: " + response.getStatus + ": " + responseContent)
    }
  }
  def retrieveConfig() = {
    val response = resource.get(classOf[ClientResponse])
    val responseContent = IOUtils.toString(response.getEntityInputStream, "UTF-8")

    response.getStatus match {
      case 200 => responseContent
      case _   => throw new RuntimeException("Unexpected response: " + response.getStatus + ": " + responseContent)
    }
  }
}