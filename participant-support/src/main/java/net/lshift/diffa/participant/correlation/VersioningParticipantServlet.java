package net.lshift.diffa.participant.correlation;

import net.lshift.diffa.participant.common.JSONHelper;
import net.lshift.diffa.participant.common.ServletHelper;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Base support servlet for implementing a Diffa scanning participant.
 */
public abstract class VersioningParticipantServlet
    extends HttpServlet
    implements VersioningParticipantHandler {
  
  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {

    String body = req.getParameter("body");
    if (body == null) {
      resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      ServletHelper.writeResponse(resp, "Missing body parameter");
    } else {
      ProcessingResponse response = generateVersion(body);

      resp.setStatus(HttpServletResponse.SC_OK);
      JSONHelper.writeProcessingResponse(resp.getOutputStream(), response);
    }
  }

  /**
   * Callback to be implemented by sub-classes to generate version recovery information for correlated systems.
   * @param entityBody the body of the entity from the upstream.
   * @return the processing response containing versioning information.
   */
  @Override
  public abstract ProcessingResponse generateVersion(String entityBody);
}
