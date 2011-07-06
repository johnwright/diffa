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
import org.joda.time.DateTime
import scala.collection.JavaConversions._
import net.lshift.diffa.kernel.differencing.AttributesUtil
import net.lshift.diffa.agent.itest.support.TestConstants._
import net.lshift.diffa.kernel.config.{RepairAction, EscalationEvent, EscalationOrigin, EscalationActionType, RangeCategoryDescriptor}
import org.restlet.data.Protocol
import org.restlet.routing.Router
import org.restlet.resource.{ServerResource, Post}
import org.restlet.{Application, Component}
import collection.mutable.HashMap
import org.junit.Before
import net.lshift.diffa.agent.client._

/**
 * An assembled environment consisting of a downstream and upstream participant. Provides a factory for the
 * various parts, along with convenience methods for making the configuration valid.
 */
class TestEnvironment(val pairKey: String,
                      val participants: Participants,
                      changesClientBuilder: TestEnvironment => ChangesClient,
                      versionScheme: VersionScheme) {

  // Keep a tally of the amount of requested resends
  val entityResendTally = new HashMap[String,Int]
  @Before def resetTally = entityResendTally.clear()

  private val repairActionsComponent = {
    val component = new Component
    component.getServers.add(Protocol.HTTP, 8123)
    component.getDefaultHost.attach("/repair", new RepairActionsApplication(entityResendTally))
    component
  }

  def startActionServer() {
    repairActionsComponent.start()
  }

  def stopActionServer() {
    repairActionsComponent.stop()
  }

  def withActionsServer(op: => Unit) {
    try {
      startActionServer()
      op
    }
    finally {
      stopActionServer()
    }
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
  val escalationsClient:EscalationsClient = new EscalationsRestClient(serverRoot)
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

  // Escalations
  val escalationName = "Repair By Resending"

  // Categories
  val categories = Map("bizDate" -> new RangeCategoryDescriptor("datetime"))
  
  // Participants' RPC server setup
  participants.startUpstreamServer(upstream)
  participants.startDownstreamServer(downstream)

  // Ensure that the configuration exists
  configurationClient.declareGroup("g1")
  configurationClient.declareEndpoint(upstreamEpName, participants.upstreamUrl, contentType, participants.inboundUrl, contentType, true, categories)
  configurationClient.declareEndpoint(downstreamEpName, participants.downstreamUrl, contentType, participants.inboundUrl, contentType, true, categories)
  configurationClient.declareRepairAction(entityScopedActionName, entityScopedActionUrl, RepairAction.ENTITY_SCOPE, pairKey)
  configurationClient.declareEscalation(escalationName, pairKey, entityScopedActionName, EscalationActionType.REPAIR, EscalationEvent.DOWNSTREAM_MISSING, EscalationOrigin.SCAN)
  createPair

  def createPair = configurationClient.declarePair(pairKey, versionScheme.policyName, matchingTimeout, upstreamEpName, downstreamEpName, "g1")
  def deletePair() {
   configurationClient.deletePair(pairKey)
  }
  def createPairScopedAction = configurationClient.declareRepairAction(pairScopedActionName, pairScopedActionUrl, RepairAction.PAIR_SCOPE, pairKey)
  
  // Participants' RPC client setup
  val upstreamClient: UpstreamParticipant = participants.upstreamClient
  val downstreamClient: DownstreamParticipant = participants.downstreamClient


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

  def addAndNotifyUpstream(id:String, attributes:Map[String, String], content:String) {
    upstream.addEntity(id, attributes, Placeholders.dummyLastUpdated, content)
    changesClient.onChangeEvent(new UpstreamChangeEvent(upstreamEpName, id, AttributesUtil.toSeq(attributes), Placeholders.dummyLastUpdated, versionForUpstream(content)))
  }
  def addAndNotifyDownstream(id:String, attributes:Map[String, String], content:String) {
    downstream.addEntity(id, attributes, Placeholders.dummyLastUpdated, content)
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

  /**
   * Update the resend tally for each entity
   */
  @Post def resend = {
    val entityId = getRequest.getResourceRef.getLastSegment
    val tally = getContext.getAttributes.get("tally").asInstanceOf[HashMap[String,Int]]
    tally.get(entityId) match {
      case Some(x) => tally(entityId) = x + 1
      case None    => tally(entityId) = 1
    }
    "resending entity"
  }
}
class RepairActionsApplication(tally:HashMap[String,Int]) extends Application {
  override def createInboundRoot = {

    // Pass the tally to the underlying resource
    // NOTE: This is due to Restlets API - it feels like they should provide a resource that you can constructor inject
    getContext.setAttributes(Map("tally"-> tally))

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
