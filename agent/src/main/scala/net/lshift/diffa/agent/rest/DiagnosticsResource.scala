package net.lshift.diffa.agent.rest

import org.springframework.stereotype.Component
import org.springframework.beans.factory.annotation.Autowired
import net.lshift.diffa.kernel.diag.DiagnosticsManager
import net.lshift.diffa.docgen.annotations.{MandatoryParams, Description}
import net.lshift.diffa.docgen.annotations.MandatoryParams.MandatoryParam
import javax.ws.rs.{PathParam, Produces, GET, Path}
import javax.ws.rs.core.Response

/**
 * Resource providing REST-based access to diagnostic data.
 */
@Path("/diagnostics")
@Component
class DiagnosticsResource {
  @Autowired var diagnostics: DiagnosticsManager = null

  @GET
  @Path("/{pairKey}/log")
  @Produces(Array("application/json"))
  @Description("Retrieves the recent log entries for the given pair.")
  @MandatoryParams(Array(new MandatoryParam(name = "pairKey", datatype = "string", description = "Pair Key")))
  def getPairStates(@PathParam("pairKey") pairKey: String): Response = {
    val events = diagnostics.queryEvents(pairKey)
    Response.ok(scala.collection.JavaConversions.seqAsJavaList(events)).build
  }
}