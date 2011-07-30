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

import javax.ws.rs._
import core._
import org.slf4j.{Logger, LoggerFactory}
import net.lshift.diffa.docgen.annotations.{OptionalParams, MandatoryParams, Description}
import net.lshift.diffa.docgen.annotations.MandatoryParams.MandatoryParam
import net.lshift.diffa.kernel.participants.ParticipantType
import scala.collection.JavaConversions._
import org.joda.time.format.{ISODateTimeFormat, DateTimeFormat}
import net.lshift.diffa.kernel.differencing.{SessionScope, SessionManager, SessionEvent}
import org.joda.time.{DateTime, Interval}
import net.lshift.diffa.docgen.annotations.OptionalParams.OptionalParam

class DifferencesResource(val sessionManager: SessionManager,
                          val domain:String,
                          val uriInfo:UriInfo) {

  private val log: Logger = LoggerFactory.getLogger(getClass)

  val parser = DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss Z");
  val isoDateTime = ISODateTimeFormat.basicDateTimeNoMillis

  @POST
  @Path("/sessions")
  @Description("Returns the URL of an endpoint that can be polled to receive outstanding differences. " +
    "If a requested pair does not exist, a 404 will be returned.")
  @OptionalParams(Array(
    new OptionalParam(name = "pairs", datatype = "string", description = "Comma-separated list of pair IDs"),
    new OptionalParam(name = "start", datatype = "date", description = "This is the lower bound of the date range for analysis"),
    new OptionalParam(name = "end", datatype = "date", description = "This is the upper bound of the date range for analysis")
  ))
  def subscribe(@QueryParam("pairs") pairs: String,
                @QueryParam("start") start: String,
                @QueryParam("end") end: String) = {
    val scope = pairs match {
      case null => SessionScope.all
      case _ => SessionScope.forPairs(domain, pairs.split(","): _*)
    }
    if (log.isTraceEnabled) {
      log.debug("Creating a subscription for this scope: " + scope)
    }
    val sessionId = sessionManager.start(scope)
    val uri = uriInfo.getBaseUriBuilder.path("diffs/sessions/" + sessionId).build()
    Response.created(uri).`type`("text/plain").build()
  }

  @POST
  @Path("/sessions/{sessionId}/scan")
  @Description("Forces Diffa to execute a scan on the pairs underlying the session")
  @MandatoryParams(Array(new MandatoryParam(name="sessionId", datatype="string", description="Session ID")))
  def scanAllInSession(@PathParam("sessionId") sessionId:String, @Context request:Request) : Response = {
    if (log.isTraceEnabled) {
      log.trace("Scan requested for sessionId = " + sessionId)
    }
    sessionManager.runScan(sessionId)
    Response.status(Response.Status.ACCEPTED).build
  }

  @POST
  @Path("/sessions/scan_all")
  @Description("Forces Diffa to execute a scan operation for every configured pair.")
  def scanAllPairings = {
    log.info("Initiating scan of all known pairs")
    sessionManager.runScanForAllPairings
  }

  @GET
  @Path("/sessions/all_scan_states")
  @Description("Lists the scanning state for every configured pair.")
  def getAllPairStates = {
    val states = sessionManager.retrieveAllPairScanStates
    Response.ok(scala.collection.JavaConversions.mapAsJavaMap(states)).build
  }

  @GET
  @Path("/sessions/{sessionId}/scan")
  @Produces(Array("application/json"))
  @Description("Retrieves the scan states of pairs in the current session.")
  @MandatoryParams(Array(new MandatoryParam(name = "sessionId", datatype = "string", description = "Session ID")))
  def getPairStates(@PathParam("sessionId") sessionId: String): Response = {
    val states = sessionManager.retrievePairScanStates(sessionId)
    Response.ok(scala.collection.JavaConversions.mapAsJavaMap(states)).build
  }

  @GET
  @Path("/sessions/{sessionId}")
  @Produces(Array("application/json"))
  @Description("Returns a list of outstanding differences in the current session in a paged format.")
  @MandatoryParams(Array(
    new MandatoryParam(name = "sessionId", datatype = "string", description = "Session ID"),
    new MandatoryParam(name = "pairKey", datatype = "string", description = "Pair Key")))
  @OptionalParams(Array(
    new OptionalParam(name = "range-start", datatype = "date", description = "The lower bound of the items to be paged."),
    new OptionalParam(name = "range-end", datatype = "date", description = "The upper bound of the items to be paged."),
    new OptionalParam(name = "offset", datatype = "int", description = "The offset to base the page on."),
    new OptionalParam(name = "length", datatype = "int", description = "The number of items to return in the page.")))
  def getSessionEvents(@PathParam("sessionId") sessionId: String,
                       @QueryParam("pairKey") pairKey:String,
                       @QueryParam("range-start") from_param:String,
                       @QueryParam("range-end") until_param:String,
                       @QueryParam("offset") offset_param:String,
                       @QueryParam("length") length_param:String,
                       @Context request: Request) = {

    val now = new DateTime()
    val from = defaultDateTime(from_param, now.minusDays(1))
    val until = defaultDateTime(until_param, now)
    val offset = defaultInt(offset_param, 0)
    val length = defaultInt(length_param, 100)

    try {
      val sessionVsn = new EntityTag(sessionManager.retrieveSessionVersion(sessionId))

      request.evaluatePreconditions(sessionVsn) match {
        case null => // We'll continue with the request
        case r => throw new WebApplicationException(r.build)
      }

      val interval = new Interval(from,until)
      val diffs = sessionManager.retrievePagedEvents(sessionId, pairKey, interval, offset, length)
      Response.ok(diffs.toArray).tag(sessionVsn).build
    }
    catch {
      case e:NoSuchElementException =>
        log.error("Unsucessful query on sessionId = " + sessionId, e)
        throw new WebApplicationException(404)
    }
  }
  
  @GET
  @Path("/sessions/{sessionId}/zoom")
  @Produces(Array("application/json"))
  @MandatoryParams(Array(
      new MandatoryParam(name = "range-start", datatype = "date", description = "The starting time for any differences"),
      new MandatoryParam(name = "range-end", datatype = "date", description = "The ending time for any differences"),
      new MandatoryParam(name = "bucketing", datatype = "int", description = "The size in elements in the zoomed view")))
  @Description("Returns a zoomed view of the data within a specific time range")
  def getZoomedView(@PathParam("sessionId") sessionId: String,
                    @QueryParam("range-start") rangeStart: String,
                    @QueryParam("range-end") rangeEnd:String,
                    @QueryParam("bucketing") width:Int,
                    @Context request: Request): Response = {
    try {
      // Evaluate whether the version of the session has changed
      val sessionVsn = new EntityTag(sessionManager.retrieveSessionVersion(sessionId))
      request.evaluatePreconditions(sessionVsn) match {
        case null => // We'll continue with the request
        case r => throw new WebApplicationException(r.build)
      }

      val rangeStartDate = isoDateTime.parseDateTime(rangeStart)
      val rangeEndDate = isoDateTime.parseDateTime(rangeEnd)

      if (rangeStartDate == null || rangeEndDate == null) {
        return Response.status(Response.Status.BAD_REQUEST).entity("Invalid start or end date").build
      }

      // Calculate the maximum number of buckets that will be seen
      val rangeSecs = (rangeEndDate.getMillis - rangeStartDate.getMillis) / 1000
      val max = (rangeSecs.asInstanceOf[Double] / width.asInstanceOf[Double]).ceil.asInstanceOf[Int]
      if (max > 100) {
        return Response.status(Response.Status.BAD_REQUEST).entity("Time range too big for width. Maximum of 100 blobs can be generated, requesting " + max).build
      }

      // Calculate the zoomed view
      val interestingEvents = sessionManager.retrieveAllEventsInInterval(sessionId, new Interval(rangeStartDate, rangeEndDate))

      // Bucket the events
      val pairs = scala.collection.mutable.Map[String, ZoomPair]()
      interestingEvents.foreach(evt => {
        val pair = pairs.getOrElseUpdate(evt.objId.pairKey, new ZoomPair(evt.objId.pairKey, rangeStartDate, width, max))
        pair.addEvent(evt)
      })

      // Convert to an appropriate web response
      val respObj = asJavaMap(pairs.keys.map(pair => pair -> pairs(pair).toArray).toMap[String, Array[Int]])

      Response.ok(respObj).tag(sessionVsn).build
    }
    catch {
      case e: NoSuchElementException => {
        log.error("Unsucessful query on sessionId = " + sessionId)
        throw new WebApplicationException(404)
      }
    }



  }

  class ZoomPair(pairKey:String, rangeStart:DateTime, width:Int, max:Int) {
    private val buckets = new Array[Int](max)

    def addEvent(evt:SessionEvent) = {
      val offset = (evt.detectedAt.getMillis - rangeStart.getMillis) / 1000
      val bucketNum = (offset / width).asInstanceOf[Int]

      // Add an entry to the bucket
      buckets(bucketNum) += 1
    }

    def toArray:Array[Int] = buckets
  }

  @GET
  @Path("/events/{sessionId}/{evtSeqId}/{participant}")
  @Produces(Array("text/plain"))
  @Description("Returns the verbatim detail from each participant for the event that corresponds to the sequence id.")
  @MandatoryParams(Array(
    new MandatoryParam(name = "sessionId", datatype = "string", description = "Session ID"),
    new MandatoryParam(name = "evtSeqId", datatype = "string", description = "Event Sequence ID"),
    new MandatoryParam(name = "participant", datatype = "string", description = "Denotes whether the upstream or downstream participant is intended. Legal values are {upstream,downstream}.")
  ))
  def getDetail(@PathParam("sessionId") sessionId:String,
                @PathParam("evtSeqId") evtSeqId:String,
                @PathParam("participant") participant:String) : String =
    sessionManager.retrieveEventDetail(sessionId, evtSeqId, ParticipantType.withName(participant))

  def defaultDateTime(input:String, default:DateTime) = input match {
    case "" | null => default
    case x         => isoDateTime.parseDateTime(x)
  }

  def defaultInt(input:String, default:Int) = input match {
    case "" | null => default
    case x         => x.toInt
  }
}
