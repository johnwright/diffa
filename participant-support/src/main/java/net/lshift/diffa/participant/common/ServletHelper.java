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
