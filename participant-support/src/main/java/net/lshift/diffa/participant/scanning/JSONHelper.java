package net.lshift.diffa.participant.scanning;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Helper for serialising scanning results in JSON.
 */
public class JSONHelper {
  private static ObjectMapper mapper = new ObjectMapper();
  static {
    mapper.getSerializationConfig().set(SerializationConfig.Feature.WRITE_DATES_AS_TIMESTAMPS, false);
  }

  public static void writeQueryResult(OutputStream responseStream, Iterable<QueryResultEntry> entries)
      throws IOException {
    try {
      mapper.writeValue(responseStream, entries);
    } catch (IOException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new IOException("Failed to serialise result to JSON", ex);
    }
  }
}
