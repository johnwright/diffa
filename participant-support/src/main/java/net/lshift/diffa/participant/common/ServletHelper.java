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
package net.lshift.diffa.participant.common;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Helper for the various participant servlets.
 */
public final class ServletHelper {
  /**
   * Write a string response out in UTF-8 to a servlet response.
   * @param response the servlet response to fill.
   * @param content the string content.
   * @throws IOException if the write-out fails.
   */
  public static void writeResponse(HttpServletResponse response, String content) throws IOException {
    OutputStream output = response.getOutputStream();
    try {
      output.write(content.getBytes("UTF-8"));
    } finally {
      output.close();
    }
  }
}
