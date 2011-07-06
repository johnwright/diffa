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
