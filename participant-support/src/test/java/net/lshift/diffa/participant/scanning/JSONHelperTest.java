package net.lshift.diffa.participant.scanning;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the JSON serialisation support.
 */
public class JSONHelperTest {
  @Test
  public void shouldSerialiseEmptyList() throws Exception {
    String emptyRes = serialiseResult(new ArrayList<ScanResultEntry>());
    assertJSONEquals("[]", emptyRes);
  }

  @Test
  public void shouldSerialiseSingleEntityWithNoAttributes() throws Exception {
    String single = serialiseResult(Arrays.asList(
      ScanResultEntry.forEntity("id1", "v1", new DateTime(2011, 6, 5, 15, 3, 0, 0, DateTimeZone.UTC))));
    assertJSONEquals("[{\"id\":\"id1\",\"version\":\"v1\",\"lastUpdated\":\"2011-06-05T15:03:00.000Z\"}]", single);
  }

  @Test
  public void shouldSerialiseSingleEntityWithAttributes() throws Exception {
    String single = serialiseResult(Arrays.asList(
      ScanResultEntry.forEntity("id1", "v1", new DateTime(2011, 6, 5, 15, 3, 0, 0, DateTimeZone.UTC),
          generateAttributes("a1v1", "a2v2"))));
    assertJSONEquals(
      "[{\"id\":\"id1\",\"attributes\":{\"a1\":\"a1v1\",\"a2\":\"a2v2\"},\"version\":\"v1\",\"lastUpdated\":\"2011-06-05T15:03:00.000Z\"}]",
      single);
  }

  private static String serialiseResult(Iterable<ScanResultEntry> entries) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    JSONHelper.writeQueryResult(baos, entries);

    return new String(baos.toByteArray(), "UTF-8");
  }

  private static Map<String, String> generateAttributes(String a1, String a2) {
    Map<String, String> result = new HashMap<String, String>();
    result.put("a1", a1);
    result.put("a2", a2);
    return result;
  }

  private static void assertJSONEquals(String expected, String actual) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode expectedTree = mapper.readTree(expected);
    JsonNode actualTree = mapper.readTree(actual);

    assertEquals(expectedTree, actualTree);
  }
}
