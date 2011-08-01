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
import org.restlet.data.Protocol
import org.restlet.routing.Router
import org.restlet.resource.{ServerResource, Post}
import net.lshift.diffa.kernel.config._
import org.restlet.{Application, Component}
import collection.mutable.HashMap
import net.lshift.diffa.agent.client._
import java.util.List
import net.lshift.diffa.participant.scanning.{ScanAggregation, ScanConstraint}
import net.lshift.diffa.kernel.frontend.PairDef

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
  val scanningClient:ScanningClient = new ScanningRestClient(serverRoot)

  // Domain
  val domain = "domain"

  // Participants
  val upstreamEpName = pairKey + "-us"
  val downstreamEpName = pairKey + "-ds"
  val upstream = new UpstreamMemoryParticipant(versionScheme.upstreamVersionGen) {
    var queryResponseDelay = 0

    override protected def doQuery(constraints: List[ScanConstraint], aggregations: List[ScanAggregation]) = {
      if (queryResponseDelay > 0)
        Thread.sleep(queryResponseDelay)

      super.doQuery(constraints, aggregations)
    }
  }
  val downstream = new DownstreamMemoryParticipant(versionScheme.upstreamVersionGen, versionScheme.downstreamVersionGen)

  // Actions
  val entityScopedActionName = "Resend Source"
  val entityScopedActionUrl = "http://localhost:8123/repair/resend/{id}"
  val pairScopedActionName = "Resend All"
  val pairScopedActionUrl = "http://localhost:8123/repair/resend-all"

  // Escalations
  val escalationName = "Repair By Resending"

  // Categories
  val categories = Map("someDate" -> new RangeCategoryDescriptor("datetime"), "someString" -> new SetCategoryDescriptor(Set("ss")))
  
  // Participants' RPC server setup
  participants.startUpstreamServer(upstream, upstream)
  participants.startDownstreamServer(downstream, downstream, downstream)

  // Ensure that the configuration exists
  configurationClient.declareEndpoint(Endpoint(name = upstreamEpName,
    scanUrl = participants.upstreamScanUrl, contentRetrievalUrl = participants.upstreamContentUrl, contentType = contentType,
    inboundUrl = participants.inboundUrl, inboundContentType = contentType,
    categories = categories))
  configurationClient.declareEndpoint(Endpoint(name = downstreamEpName,
    scanUrl = participants.downstreamScanUrl, contentRetrievalUrl = participants.downstreamContentUrl,
    versionGenerationUrl = participants.downstreamVersionUrl, contentType = contentType,
    inboundUrl = participants.inboundUrl, inboundContentType = contentType,
    categories = categories))
  configurationClient.declareRepairAction(entityScopedActionName, entityScopedActionUrl, RepairAction.ENTITY_SCOPE, pairKey)
  configurationClient.declareEscalation(escalationName, pairKey, entityScopedActionName, EscalationActionType.REPAIR, EscalationEvent.DOWNSTREAM_MISSING, EscalationOrigin.SCAN)
  createPair

  def createPair = configurationClient.declarePair(PairDef(pairKey, versionScheme.policyName, matchingTimeout, upstreamEpName, downstreamEpName, "0 15 10 15 * ?"))
  def deletePair() {
   configurationClient.deletePair(pairKey)
  }
  def createPairScopedAction = configurationClient.declareRepairAction(pairScopedActionName, pairScopedActionUrl, RepairAction.PAIR_SCOPE, pairKey)
  
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
    val attributes = pack(someDate = someDate, someString = someString)

    upstream.addEntity(id, someDate, someString, Placeholders.dummyLastUpdated, content)
    changesClient.onChangeEvent(new UpstreamChangeEvent(upstreamEpName, id, AttributesUtil.toSeq(attributes), Placeholders.dummyLastUpdated, versionForUpstream(content)))
  }
  def addAndNotifyDownstream(id:String, content:String, someDate:DateTime, someString:String) {
    val attributes = pack(someDate = someDate, someString = someString)

    downstream.addEntity(id, someDate, someString, Placeholders.dummyLastUpdated, content)
    versionScheme match {
      case SameVersionScheme =>
        changesClient.onChangeEvent(new DownstreamChangeEvent(downstreamEpName, id, AttributesUtil.toSeq(attributes), Placeholders.dummyLastUpdated, versionForDownstream(content)))
      case CorrelatedVersionScheme =>
        changesClient.onChangeEvent(new DownstreamCorrelatedChangeEvent(downstreamEpName, id, AttributesUtil.toSeq(attributes), Placeholders.dummyLastUpdated,
          versionForUpstream(content), versionForDownstream(content)))
    }
  }

  def pack(someDate:DateTime, someString:String) = Map("someDate" -> someDate.toString(), "someString" -> someString)

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
