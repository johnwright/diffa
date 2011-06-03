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

package net.lshift.diffa.agent.itest.config

import org.junit.Test
import org.junit.Assert._
import net.lshift.diffa.agent.itest.support.TestConstants._
import net.lshift.diffa.messaging.json.NotFoundException
import com.eaio.uuid.UUID
import net.lshift.diffa.tools.client.{ConfigurationRestClient, ScanningRestClient}
import net.lshift.diffa.kernel.config.RangeCategoryDescriptor
import scala.collection.JavaConversions._

/**
 * Smoke tests for the scan interface.
 */
class ScanningTest {

  val scanClient = new ScanningRestClient(agentURL)
  val configClient = new ConfigurationRestClient(agentURL)

  @Test(expected = classOf[NotFoundException])
  def nonExistentPairShouldGenerateNotFoundError = {
    scanClient.cancelScanning(new UUID().toString)
  }

  def existentPairShouldNotGenerateError = {
    val up = new UUID().toString
    val down = new UUID().toString
    val pair = new UUID().toString
    val group = new UUID().toString

    val categories = Map("bizDate" -> new RangeCategoryDescriptor("datetime"))

    configClient.declareEndpoint(up, "http://upstream.com", "application/json", null,null, true, categories)
    configClient.declareEndpoint(down, "http://downstream.com", "application/json", null,null, true, categories)
    configClient.declareGroup(group)
    configClient.declarePair(pair, "same", 1, up, down, group)

    // Simple smoke test - you could kick off a scan and verify that it gets interrupted,
    // but this code path is tested in the unit test

    assertTrue(scanClient.cancelScanning(pair))
  }
}