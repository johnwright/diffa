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

package net.lshift.diffa.agent.itest.support

import net.lshift.diffa.kernel.events.{DownstreamCorrelatedChangeEvent, DownstreamChangeEvent, UpstreamChangeEvent}
import net.lshift.diffa.kernel.participants.{UpstreamMemoryParticipant, DownstreamMemoryParticipant, UpstreamParticipant, DownstreamParticipant}
import net.lshift.diffa.kernel.client._
import net.lshift.diffa.kernel.util.Placeholders
import net.lshift.diffa.agent.client.{ConfigurationRestClient, DifferencesRestClient, ActionsRestClient, UsersRestClient}
import org.joda.time.DateTime
import scala.collection.JavaConversions._
import net.lshift.diffa.kernel.differencing.AttributesUtil
import net.lshift.diffa.agent.itest.support.TestConstants._
import org.restlet.data.Protocol
import org.restlet.routing.Router
import org.restlet.{Application, Component}
import org.restlet.resource.{ServerResource, Post}
import net.lshift.diffa.kernel.config._

/**
 * An assembled environment consisting of a downstream and upstream participant. Provides a factory for the
 * various parts, along with convenience methods for making the configuration valid.
 */
class TestEnvironment(val pairKey: String,
                      val participants: Participants,
                      changesClientBuilder: TestEnvironment => ChangesClient,
                      versionScheme: VersionScheme) {

  private val repairActionsComponent = {
    val component = new Component
    component.getServers.add(Protocol.HTTP, 8123)
    component.getDefaultHost.attach("/repair", new RepairActionsApplication)
    component
  }

  def startActionServer() {
    repairActionsComponent.start()
  }

  def stopActionServer() {
    repairActionsComponent.stop()
  }

  def serverRoot = agentURL
  val contentType = "application/json"
  val matchingTimeout = 1  // 1 second

  // Version Generation
  val versionForUpstream = versionScheme.upstreamVersionGen
  val versionForDownstream = versionScheme.downstreamVersionGen

  // Clients
  val configurationClient:ConfigurationClient = new ConfigurationRestClient(serverRoot)
  val diffClient:DifferencesClient = new DifferencesRestClient(serverRoot)
  val actionsClient:ActionsClient = new ActionsRestClient(serverRoot)
  val changesClient:ChangesClient = changesClientBuilder(this)
  val usersClient:UsersClient = new UsersRestClient(serverRoot)

  // Participants
  val upstreamEpName = pairKey + "-us"
  val downstreamEpName = pairKey + "-ds"
  val upstream = new UpstreamMemoryParticipant(versionScheme.upstreamVersionGen)
  val downstream = new DownstreamMemoryParticipant(versionScheme.upstreamVersionGen, versionScheme.downstreamVersionGen)

  // Actions
  val entityScopedActionName = "Resend Source"
  val entityScopedActionUrl = "http://localhost:8123/repair/resend/{id}"
  val pairScopedActionName = "Resend All"
  val pairScopedActionUrl = "http://localhost:8123/repair/resend-all"

  // Categories
  val categories = Map("someDate" -> new RangeCategoryDescriptor("datetime"), "someString" -> new SetCategoryDescriptor(Set("ss")))
  
  // Participants' RPC server setup
  participants.startUpstreamServer(upstream, upstream, upstream)
  participants.startDownstreamServer(downstream, downstream, downstream, downstream)

  // Ensure that the configuration exists
  configurationClient.declareGroup("g1")
  configurationClient.declareEndpoint(Endpoint(name = upstreamEpName,
    url = participants.upstreamUrl, scanUrl = participants.upstreamScanUrl, contentRetrievalUrl = participants.upstreamContentUrl, contentType = contentType,
    inboundUrl = participants.inboundUrl, inboundContentType = contentType,
    categories = categories))
  configurationClient.declareEndpoint(Endpoint(name = downstreamEpName,
    url = participants.downstreamUrl, scanUrl = participants.downstreamScanUrl, contentRetrievalUrl = participants.downstreamContentUrl,
    versionGenerationUrl = participants.downstreamVersionUrl, contentType = contentType,
    inboundUrl = participants.inboundUrl, inboundContentType = contentType,
    categories = categories))
  configurationClient.declareRepairAction(entityScopedActionName, entityScopedActionUrl, RepairAction.ENTITY_SCOPE, pairKey)
  createPair

  def createPair = configurationClient.declarePair(PairDef(pairKey, versionScheme.policyName, matchingTimeout, upstreamEpName, downstreamEpName, "g1"))
  def deletePair() {
   configurationClient.deletePair(pairKey)
  }
  def createPairScopedAction = configurationClient.declareRepairAction(pairScopedActionName, pairScopedActionUrl, RepairAction.PAIR_SCOPE, pairKey)
  
  // Participants' RPC client setup
//  val upstreamClient: UpstreamParticipant = participants.upstreamClient
//  val downstreamClient: DownstreamParticipant = participants.downstreamClient


  val username = "foo"
  val mail = "foo@bar.com"
  usersClient.declareUser(username,mail)

  /**
   * Requests that the environment remove all stored state from the participants.
   */
  def clearParticipants() {
    upstream.clearEntities
    downstream.clearEntities
  }

  def addAndNotifyUpstream(id:String, content:String, someDate:DateTime, someString:String) {
    val attributes = Map("someDate" -> someDate.toString(), "someString" -> someString)

    upstream.addEntity(id, someDate, someString, Placeholders.dummyLastUpdated, content)
    changesClient.onChangeEvent(new UpstreamChangeEvent(upstreamEpName, id, AttributesUtil.toSeq(attributes), Placeholders.dummyLastUpdated, versionForUpstream(content)))
  }
  def addAndNotifyDownstream(id:String, content:String, someDate:DateTime, someString:String) {
    val attributes = Map("someDate" -> someDate.toString(), "someString" -> someString)

    downstream.addEntity(id, someDate, someString, Placeholders.dummyLastUpdated, content)
    versionScheme match {
      case SameVersionScheme =>
        changesClient.onChangeEvent(new DownstreamChangeEvent(downstreamEpName, id, AttributesUtil.toSeq(attributes), Placeholders.dummyLastUpdated, versionForDownstream(content)))
      case CorrelatedVersionScheme =>
        changesClient.onChangeEvent(new DownstreamCorrelatedChangeEvent(downstreamEpName, id, AttributesUtil.toSeq(attributes), Placeholders.dummyLastUpdated,
          versionForUpstream(content), versionForDownstream(content)))
    }
  }

  // TODO Maybe this can be accomplished using an implicit definition somewhere
  def bizDate(d:DateTime) = Map("bizDate" -> d.toString())
  def bizDateValues(d:DateTime) = Seq(d.toString())

}

class ResendAllResource extends ServerResource {
  @Post def resendAll = "resending all"
}
class ResendEntityResource extends ServerResource {
  @Post def resend = "resending entity"
}
class RepairActionsApplication extends Application {
  override def createInboundRoot = {
    val router = new Router(getContext)
    router.attach("/resend-all", classOf[ResendAllResource])
    router.attach("/resend/abc", classOf[ResendEntityResource])
    router
  }
}

abstract class VersionScheme {
  def policyName:String
  def upstreamVersionGen:Function1[String, String]
  def downstreamVersionGen:Function1[String, String]
}
object SameVersionScheme extends VersionScheme {
  val policyName = "same"
  val upstreamVersionGen = (content) => "vsn_" + content
  val downstreamVersionGen = (content) => "vsn_" + content
}
object CorrelatedVersionScheme extends VersionScheme {
  val policyName = "correlated"
  val upstreamVersionGen = (content) => "uvsn_" + content
  val downstreamVersionGen = (content) => "dvsn_" + content
}
