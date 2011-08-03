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

import org.joda.time.DateTime
import com.sun.jersey.core.util.MultivaluedMapImpl
import com.sun.jersey.api.client.{WebResource, ClientResponse}
import net.lshift.diffa.kernel.client.DifferencesClient
import net.lshift.diffa.kernel.participants.ParticipantType
import javax.ws.rs.core.{Response, MediaType}
import net.lshift.diffa.kernel.differencing.{PairScanState, SessionScope, SessionEvent}
import scala.collection.JavaConversions._
import net.lshift.diffa.messaging.json.{NotFoundException, AbstractRestClient}
import org.joda.time.format.ISODateTimeFormat
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.node.ObjectNode

/**
 * A RESTful client to start a matching session and poll for events from it.
 */
class DifferencesRestClient(serverRootUrl:String)
    extends AbstractRestClient(serverRootUrl, "rest/diffs/")
        with DifferencesClient {
  val supportsStreaming = false
  val supportsPolling = true

  val formatter = ISODateTimeFormat.basicDateTimeNoMillis

  /**
   * Creates a differencing session that can be polled for match events.
   * @return The id of the session containing the events
   */
  def subscribe(scope:SessionScope, start:DateTime, end:DateTime) : String = {
    val params = new MultivaluedMapImpl()
    params.add("start", f(start) )
    params.add("end", f(end) )
    params.add("pairs", scope.includedPairs.foldLeft("") {
      case ("", p)  => p
      case (acc, p) => acc + "," + p
    })

    val path = resource.path("sessions").queryParams(params)
    val response = path.post(classOf[ClientResponse])

    val status = response.getClientResponseStatus

    status.getStatusCode match {
      case 201     => response.getLocation.toString.split("/").last
      case x:Int   => handleHTTPError(x, path, status)
    }
  }

  def runScan(sessionId: String) = {
    val p = resource.path("sessions").path(sessionId).path("scan")
    val response = p.accept(MediaType.APPLICATION_JSON_TYPE).post(classOf[ClientResponse])
    val status = response.getClientResponseStatus
    status.getStatusCode match {
      case 202     => // Successfully submitted (202 is "Accepted")
      case x:Int   => throw new RuntimeException("HTTP " + x + " : " + status.getReasonPhrase)
    }
  }

  def getScanStatus(sessionId: String) = {
    val path = resource.path("sessions").path(sessionId).path("scan")
    val media = path.accept(MediaType.APPLICATION_JSON_TYPE)
    val response = media.get(classOf[ClientResponse])

    val status = response.getClientResponseStatus

    status.getStatusCode match {
      case 200 => {
        val responseData = response.getEntity(classOf[java.util.Map[String, String]])
        responseData.map {case (k, v) => k -> PairScanState.valueOf(v) }.toMap
      }
      case x:Int   => handleHTTPError(x, path, status)
    }
  }

  def getZoomedView(sessionId:String, from:DateTime, until:DateTime, bucketing:Int)  = {
    val path = resource.path("sessions").path(sessionId).path("zoom")
                        .queryParam("range-start", formatter.print(from))
                        .queryParam("range-end", formatter.print(until))
                        .queryParam("bucketing", bucketing.toString)
    val media = path.accept(MediaType.APPLICATION_JSON_TYPE)
    val response = media.get(classOf[ClientResponse])
    val status = response.getClientResponseStatus
    status.getStatusCode match {
      case 200    => response.getEntity(classOf[Map[String, Array[Int]]])
      case x:Int  => handleHTTPError(x, path, status)
    }

  }

  def handleHTTPError(x:Int, path:WebResource, status:ClientResponse.Status) =
    throw new RuntimeException("HTTP %s for resource %s ; Reason: %s".format(x, path ,status.getReasonPhrase))

  def getEvents(sessionId:String, pairKey:String, from:DateTime, until:DateTime, offset:Int, length:Int) = {
    val path = resource.path("sessions/" + sessionId)
                       .queryParam("pairKey", pairKey)
                       .queryParam("range-start", formatter.print(from))
                       .queryParam("range-end", formatter.print(until))
                       .queryParam("offset", offset.toString)
                       .queryParam("length", length.toString)
    val media = path.accept(MediaType.APPLICATION_JSON_TYPE)
    val response = media.get(classOf[ClientResponse])
    val status = response.getClientResponseStatus
    status.getStatusCode match {
      case 200 => {
        val responseMap = response.getEntity(classOf[ObjectNode])
        val diffs = responseMap.get("diffs")
        val objMapper = new ObjectMapper()

        objMapper.readValue(diffs, classOf[Array[SessionEvent]])
      }
      case x:Int   => throw new RuntimeException("HTTP " + x + " : " + status.getReasonPhrase)
    }
  }

  def eventDetail(sessionId:String, evtSeqId:String, t:ParticipantType.ParticipantType) : String = {
    val path = resource.path("events/" + sessionId + "/" + evtSeqId + "/" + t.toString )
    val media = path.accept(MediaType.TEXT_PLAIN_TYPE)
    val response = media.get(classOf[ClientResponse])
    val status = response.getClientResponseStatus
    status.getStatusCode match {
      case 200    => response.getEntity(classOf[String])
      case 404    => throw new NotFoundException(sessionId)
      case x:Int  => handleHTTPError(x, path, status)
    }
  }

  def f (d:DateTime) = {
    d match {
      case null => ""
      case _    => d.toString()
    }    
  }

  private def pollInternal(p:WebResource) : Array[SessionEvent] = {
    val media = p.accept(MediaType.APPLICATION_JSON_TYPE)
    val response = media.get(classOf[ClientResponse])

    val status = response.getClientResponseStatus

    status.getStatusCode match {
      case 200   => response.getEntity(classOf[Array[SessionEvent]])
      case x:Int   => throw new RuntimeException("HTTP " + x + " : " + status.getReasonPhrase)
    }

  }
}