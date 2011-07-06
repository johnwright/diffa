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
package net.lshift.diffa.participant.content;

import net.lshift.diffa.participant.common.ServletHelper;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Base support servlet for implementing a Diffa scanning participant.
 */
public abstract class ContentParticipantServlet extends HttpServlet {
  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {

    String identifier = req.getParameter("identifier");
    if (identifier == null) {
      resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      ServletHelper.writeResponse(resp, "Missing identifier parameter");
    } else {
      String content = retrieveContent(identifier);
      if (content == null) {
        resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
        ServletHelper.writeResponse(resp, "Identifier " + identifier + " is unknown");
      } else {
        resp.setStatus(HttpServletResponse.SC_OK);
        ServletHelper.writeResponse(resp, content);
      }
    }
  }

  /**
   * Callback to be implemented by sub-classes to retrieve the content for a given identifier.
   * @param identifier the entity identifier.
   * @return the entity content, or null if the entity is unknown.
   */
  protected abstract String retrieveContent(String identifier);
}
