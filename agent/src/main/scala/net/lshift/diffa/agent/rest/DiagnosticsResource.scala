package net.lshift.diffa.agent.rest

import net.lshift.diffa.kernel.diag.DiagnosticsManager
import net.lshift.diffa.docgen.annotations.MandatoryParams.MandatoryParam
import javax.ws.rs.core.Response
import net.lshift.diffa.docgen.annotations.{OptionalParams, MandatoryParams, Description}
import net.lshift.diffa.docgen.annotations.OptionalParams.OptionalParam
import javax.ws.rs._
import net.lshift.diffa.kernel.frontend.Configuration

/**
 * Resource providing REST-based access to diagnostic data.
 */
class DiagnosticsResource(val diagnostics: DiagnosticsManager,
                          val config: Configuration,
                          val domain:String) {


  @GET
  @Path("/{pairKey}/log")
  @Produces(Array("application/json"))
  @Description("Retrieves the most recent log entries for the given pair.")
  @MandatoryParams(Array(new MandatoryParam(name = "pairKey", datatype = "string", description = "Pair Key")))
  @OptionalParams(Array(new OptionalParam(name = "maxItems", datatype = "integer", description = "Maximum number of returned entries")))
  def getPairStates(@PathParam("pairKey") pairKey: String, @QueryParam("maxItems") maxItems:java.lang.Integer): Response = {
    val actualMaxItems = if (maxItems == null) 20 else maxItems.intValue()
    val pair = config.getPair(domain, pairKey)
    val events = diagnostics.queryEvents(pair, actualMaxItems)
    Response.ok(scala.collection.JavaConversions.seqAsJavaList(events)).build
  }
}