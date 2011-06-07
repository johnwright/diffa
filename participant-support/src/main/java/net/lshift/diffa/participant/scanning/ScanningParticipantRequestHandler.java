package net.lshift.diffa.participant.scanning;

import org.springframework.web.HttpRequestHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Base Spring HttpRequestHandler support for Diffa participants. Extends off the basic Servlet
 * implementation.
 */
public abstract class ScanningParticipantRequestHandler
    extends ScanningParticipantServlet
    implements HttpRequestHandler {

  /**
   * Delegate the Spring handleRequest implementation to the standard service dispatcher method.
   */
  @Override
  public void handleRequest(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    service(request, response);
  }
}
