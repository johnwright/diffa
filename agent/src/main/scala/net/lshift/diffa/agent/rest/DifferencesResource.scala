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
import net.lshift.diffa.kernel.differencing.{DifferencesManager, SessionEvent}
import org.joda.time.{DateTime, Interval}
import net.lshift.diffa.docgen.annotations.OptionalParams.OptionalParam

class DifferencesResource(val sessionManager: DifferencesManager,
                          val domain:String,
                          val uriInfo:UriInfo) {

  private val log: Logger = LoggerFactory.getLogger(getClass)

  val parser = DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss Z");
  val isoDateTime = ISODateTimeFormat.basicDateTimeNoMillis

  @GET
  @Produces(Array("application/json"))
  @Description("Returns a list of outstanding differences for the domain in a paged format.")
  @MandatoryParams(Array(
    new MandatoryParam(name = "pairKey", datatype = "string", description = "Pair Key")))
  @OptionalParams(Array(
    new OptionalParam(name = "range-start", datatype = "date", description = "The lower bound of the items to be paged."),
    new OptionalParam(name = "range-end", datatype = "date", description = "The upper bound of the items to be paged."),
    new OptionalParam(name = "offset", datatype = "int", description = "The offset to base the page on."),
    new OptionalParam(name = "length", datatype = "int", description = "The number of items to return in the page.")))
  def getSessionEvents(@QueryParam("pairKey") pairKey:String,
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
      val domainVsn = new EntityTag(sessionManager.retrieveDomainVersion(domain))

      request.evaluatePreconditions(domainVsn) match {
        case null => // We'll continue with the request
        case r => throw new WebApplicationException(r.build)
      }

      val interval = new Interval(from,until)
      val diffs = sessionManager.retrievePagedEvents(domain, pairKey, interval, offset, length)

      val responseObj = Map(
        "seqId" -> domainVsn.getValue,
        "diffs" -> diffs.toArray,
        "total" -> sessionManager.countEvents(domain, pairKey, interval)
      )
      Response.ok(mapAsJavaMap(responseObj)).tag(domainVsn).build
    }
    catch {
      case e:NoSuchElementException =>
        log.error("Unsucessful query on domain = " + domain, e)
        throw new WebApplicationException(404)
    }
  }
  
  @GET
  @Path("/zoom")
  @Produces(Array("application/json"))
  @MandatoryParams(Array(
      new MandatoryParam(name = "range-start", datatype = "date", description = "The starting time for any differences"),
      new MandatoryParam(name = "range-end", datatype = "date", description = "The ending time for any differences"),
      new MandatoryParam(name = "bucketing", datatype = "int", description = "The size in elements in the zoomed view")))
  @Description("Returns a zoomed view of the data within a specific time range")
  def getZoomedView(@QueryParam("range-start") rangeStart: String,
                    @QueryParam("range-end") rangeEnd:String,
                    @QueryParam("bucketing") width:Int,
                    @Context request: Request): Response = {
    try {
      // Evaluate whether the version of the session has changed
      val sessionVsn = new EntityTag(sessionManager.retrieveDomainVersion(domain))
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
      val interestingEvents = sessionManager.retrieveAllEventsInInterval(domain, new Interval(rangeStartDate, rangeEndDate))

      // Bucket the events
      val pairs = scala.collection.mutable.Map[String, ZoomPair]()
      interestingEvents.foreach(evt => {
        val pair = pairs.getOrElseUpdate(evt.objId.pair.key, new ZoomPair(evt.objId.pair.key, rangeStartDate, width, max))
        pair.addEvent(evt)
      })

      // Convert to an appropriate web response
      val respObj = mapAsJavaMap(pairs.keys.map(pair => pair -> pairs(pair).toArray).toMap[String, Array[Int]])

      Response.ok(respObj).tag(sessionVsn).build
    }
    catch {
      case e: NoSuchElementException => {
        log.error("Unsucessful query on domain = " + domain)
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
  @Path("/events/{evtSeqId}/{participant}")
  @Produces(Array("text/plain"))
  @Description("Returns the verbatim detail from each participant for the event that corresponds to the sequence id.")
  @MandatoryParams(Array(
    new MandatoryParam(name = "evtSeqId", datatype = "string", description = "Event Sequence ID"),
    new MandatoryParam(name = "participant", datatype = "string", description = "Denotes whether the upstream or downstream participant is intended. Legal values are {upstream,downstream}.")
  ))
  def getDetail(@PathParam("evtSeqId") evtSeqId:String,
                @PathParam("participant") participant:String) : String =
    sessionManager.retrieveEventDetail(domain, evtSeqId, ParticipantType.withName(participant))

  def defaultDateTime(input:String, default:DateTime) = input match {
    case "" | null => default
    case x         => isoDateTime.parseDateTime(x)
  }

  def defaultInt(input:String, default:Int) = input match {
    case "" | null => default
    case x         => x.toInt
  }
}
