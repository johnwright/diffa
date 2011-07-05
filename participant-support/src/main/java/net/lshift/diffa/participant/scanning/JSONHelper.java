package net.lshift.diffa.participant.scanning;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Helper for serialising scanning results in JSON.
 */
public class JSONHelper {
  private static ObjectMapper mapper = new ObjectMapper();
  static {
    mapper.getSerializationConfig().set(SerializationConfig.Feature.WRITE_DATES_AS_TIMESTAMPS, false);
  }

  public static void writeQueryResult(OutputStream responseStream, Iterable<ScanResultEntry> entries)
      throws IOException {
    try {
      mapper.writeValue(responseStream, entries);
    } catch (IOException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new IOException("Failed to serialise result to JSON", ex);
    }
  }

  public static ScanResultEntry[] readQueryResult(InputStream stream)
      throws IOException {
    try {
      return mapper.readValue(stream, ScanResultEntry[].class);
    } catch (IOException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new IOException("Failed to deserialise result from JSON", ex);
    }
  }

  public static void writeProcessingResponse(OutputStream responseStream, ProcessingResponse response)
      throws IOException {
    try {
      mapper.writeValue(responseStream, response);
    } catch (IOException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new IOException("Failed to serialise result to JSON", ex);
    }
  }

  public static ProcessingResponse readProcessingResponse(InputStream stream)
      throws IOException {
    try {
      return mapper.readValue(stream, ProcessingResponse.class);
    } catch (IOException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new IOException("Failed to deserialise result from JSON", ex);
    }
  }
}
