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

import org.springframework.stereotype.Component
import javax.ws.rs._
import core.{EntityTag, Context, Request, Response}
import org.slf4j.{Logger, LoggerFactory}
import org.springframework.beans.factory.annotation.Autowired
import org.joda.time.format.DateTimeFormat
import net.lshift.diffa.docgen.annotations.{OptionalParams, MandatoryParams, Description}
import net.lshift.diffa.docgen.annotations.MandatoryParams.MandatoryParam
import net.lshift.diffa.docgen.annotations.OptionalParams.OptionalParam
import net.lshift.diffa.kernel.differencing.{SessionScope, SessionManager, SessionEvent}
import net.lshift.diffa.kernel.participants.ParticipantType

@Path("/diffs")
@Component
class DifferencesResource extends AbstractRestResource {

  private val log:Logger = LoggerFactory.getLogger(getClass)

  val parser = DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss Z");

  @Autowired var session:SessionManager = null
  
  @POST
  @Path("/sessions")
  @Description("Returns the URL of an endpoint that can be polled to receive outstanding differences. " +
               "If a requested pair does not exist, a 404 will be returned.")
  @OptionalParams(Array(
    new OptionalParam(name="pairs", datatype="string", description="Comma-separated list of pair IDs"),
    new OptionalParam(name="start", datatype="date", description="This is the lower bound of the date range for analysis"),
    new OptionalParam(name="end", datatype="date", description="This is the upper bound of the date range for analysis")
  ))
  def subscribe(@QueryParam("pairs") pairs:String,
                @QueryParam("start") start:String,
                @QueryParam("end") end:String) = {
    val scope = pairs match {
      case null => SessionScope.all
      case _    => SessionScope.forPairs(pairs.split(","):_*)
    }

    log.debug("Creating a subscription for this scope: " + scope)
    val sessionId = maybe((_:Seq[String]) => session.start(scope), scope.includedPairs)
    val uri = uriInfo.getBaseUriBuilder.path("diffs/sessions/" + sessionId).build()
    Response.created(uri).`type`("text/plain").build()
  }

  @POST
  @Path("/sessions/{sessionId}/sync")
  @Description("Forces Diffa to execute a synchronisation operation on the pairs underlying the session")
  @MandatoryParams(Array(new MandatoryParam(name="sessionId", datatype="string", description="Session ID")))
  def synchroniseSession(@PathParam("sessionId") sessionId:String, @Context request:Request) : Response = {
    log.debug("Sync requested for sessionId = " + sessionId)

    session.runSync(sessionId)
    Response.status(Response.Status.ACCEPTED).build
  }

  @POST
  @Path("/sessions/scan_all")
  @Description("Forces Diffa to execute a scan operation for every configured pair.")
  def scanAllPairings = {
    log.info("Initiating scan of all known pairs")
    session.runScanForAllPairings
  }

  @GET
  @Path("/sessions/all_scan_states")
  @Description("Lists the scanning state for every configured pair.")
  def getAllPairStates = {
    val states = session.retrieveAllPairScanStates
    Response.ok(scala.collection.JavaConversions.asJavaMap(states)).build
  }

  @GET
  @Path("/sessions/{sessionId}/sync")
  @Produces(Array("application/json"))
  @Description("Retrieves the synchronisation states of pairs in the current session.")
  @MandatoryParams(Array(new MandatoryParam(name="sessionId", datatype="string", description="Session ID")))
  def getPairStates(@PathParam("sessionId") sessionId:String): Response = {
    val states = session.retrievePairSyncStates(sessionId)
    Response.ok(scala.collection.JavaConversions.asJavaMap(states)).build
  }

  @GET
  @Path("/sessions/{sessionId}")
  @Produces(Array("application/json"))
  @Description("Returns a list of outstanding differences in the current session. ")
  @MandatoryParams(Array(new MandatoryParam(name="sessionId", datatype="string", description="Session ID")))
  @OptionalParams(Array(new OptionalParam(name="since", datatype="integer",
      description="This will return differences subsequent to the given sequence number.")))
  def getDifferences(@PathParam("sessionId") sessionId:String,
                     @QueryParam("since") since:String,
                     @Context request:Request) : Response = {
    try {
      // Evaluate whether the version of the session has changed
      val sessionVsn = new EntityTag(session.retrieveSessionVersion(sessionId))
      request.evaluatePreconditions(sessionVsn) match {
        case null => // We'll continue with the request
        case r    => throw new WebApplicationException(r.build)
      }
      
      val diffs = since match {
        case null => session.retrieveAllEvents(sessionId)
        case _    => session.retrieveEventsSince(sessionId, since)
      }

      Response.ok(diffs.toArray).tag(sessionVsn).build
    }
    catch  {
      case e:NoSuchElementException => {
        log.error("Unsucessful query on sessionId = " + sessionId + "; since = " + since)
        throw new WebApplicationException(404)
      }
    }
  }

  @GET
  @Path("/events/{sessionId}/{evtSeqId}/{participant}")
  @Produces(Array("text/plain"))
  @Description("Returns the verbatim detail from each participant for the event that corresponds to the sequence id.")
  @MandatoryParams(Array(
    new MandatoryParam(name="sessionId", datatype="string", description="Session ID"),
    new MandatoryParam(name="evtSeqId", datatype="string", description="Event Sequence ID"),
    new MandatoryParam(name="participant", datatype="string", description="Denotes whether the upstream or downstream participant is intended. Legal values are {upstream,downstream}.")
  ))
  def getDetail(@PathParam("sessionId") sessionId:String,
                @PathParam("evtSeqId") evtSeqId:String,
                @PathParam("participant") participant:String) : String = {
    log.trace("Detail params sessionId = " + sessionId + "; sequence = " + evtSeqId + "; participant = " + participant)

    try {
      session.retrieveEventDetail(sessionId, evtSeqId, ParticipantType.withName(participant))
    }
    catch {
      case e: Exception => {
        log.error("Unsucessful query on sessionId = " + sessionId + "; sequence = " + evtSeqId + " participant = " + participant, e)
        throw new WebApplicationException(404)
      }
    }

  }


  def maybe(s:String) = {
    try {
      parser.parseDateTime(s)
    }
    catch {
      case e:Exception => null 
    }
  }

}
