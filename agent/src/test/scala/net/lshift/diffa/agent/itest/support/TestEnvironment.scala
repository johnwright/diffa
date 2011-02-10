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
import net.lshift.diffa.messaging.json.{ChangesRestClient, UpstreamParticipantRestClient, DownstreamParticipantRestClient}
import net.lshift.diffa.tools.client.{ConfigurationRestClient, DifferencesRestClient, ActionsRestClient, UsersRestClient}
import collection.mutable.HashMap
import org.joda.time.DateTime
import scala.collection.JavaConversions._
import net.lshift.diffa.kernel.differencing.AttributesUtil
import net.lshift.diffa.kernel.config.{CategoryDescriptor, RangeCategoryDescriptor}

/**
 * An assembled environment consisting of a downstream and upstream participant. Provides a factory for the
 * various parts, along with convenience methods for making the configuration valid.
 */
class TestEnvironment(val pairKey: String,
                      val participants: Participants,
                      changesClientBuilder: TestEnvironment => ChangesClient,
                      versionScheme: VersionScheme) {

  val serverRoot = "http://localhost:19093/diffa-agent"
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


  // Categories
  val categories = Map("bizDate" -> new RangeCategoryDescriptor("date"))
  
  // Participants' RPC server setup
  participants.startUpstreamServer(upstream)
  participants.startDownstreamServer(downstream)

  // Ensure that the configuration exists
  configurationClient.declareGroup("g1")
  configurationClient.declareEndpoint(upstreamEpName, participants.upstreamUrl, contentType, participants.inboundUrl, contentType, true, categories)
  configurationClient.declareEndpoint(downstreamEpName, participants.downstreamUrl, contentType, participants.inboundUrl, contentType, true, categories)
  configurationClient.declarePair(pairKey, versionScheme.policyName, matchingTimeout, upstreamEpName, downstreamEpName, "g1")

  // Participants' RPC client setup
  val upstreamClient: UpstreamParticipant = participants.upstreamClient
  val downstreamClient: DownstreamParticipant = participants.downstreamClient


  val username = "foo"
  val mail = "foo@bar.com"
  usersClient.declareUser(username,mail)

  /**
   * Requests that the environment remove all stored state from the participants.
   */
  def clearParticipants {
    upstream.clearEntities
    downstream.clearEntities
  }

  def addAndNotifyUpstream(id:String, attributes:Map[String, String], content:String) = {
    upstream.addEntity(id, attributes, Placeholders.dummyLastUpdated, content)
    changesClient.onChangeEvent(new UpstreamChangeEvent(upstreamEpName, id, AttributesUtil.toSeq(attributes), Placeholders.dummyLastUpdated, versionForUpstream(content)))
  }
  def addAndNotifyDownstream(id:String, attributes:Map[String, String], content:String) = {
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