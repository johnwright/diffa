package net.lshift.diffa.participant.scanning;

import org.junit.Test;
import org.springframework.mock.web.MockHttpServletRequest;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Test cases for the AggregationBuilder.
 */
public class AggregationBuilderTest {
  @Test
  public void shouldNotAddDateAggregationForEmptyRequest() {
    MockHttpServletRequest req = new MockHttpServletRequest();
    AggregationBuilder builder = new AggregationBuilder(req);

    builder.maybeAddDateAggregation("test");
    assertEquals(0, builder.toList().size());
  }

  @Test
  public void shouldAddDateAggregationWhenParameterIsAvailable() {
    MockHttpServletRequest req = new MockHttpServletRequest();
    req.addParameter("bizDate-granularity", "monthly");
    AggregationBuilder builder = new AggregationBuilder(req);

    builder.maybeAddDateAggregation("bizDate");
    assertEquals(1, builder.toList().size());
    assertThat(builder.toList().get(0), is(instanceOf(DateAggregation.class)));
  }

  @Test
  public void shouldNotAddDateAggregationWhenDifferentParameterIsAvailable() {
    MockHttpServletRequest req = new MockHttpServletRequest();
    req.addParameter("someString-granularity", "prefix(1)");
    AggregationBuilder builder = new AggregationBuilder(req);

    builder.maybeAddDateAggregation("bizDate");
    assertEquals(0, builder.toList().size());
  }

  @Test
  public void shouldNotAddNyNameAggregationForEmptyRequest() {
    MockHttpServletRequest req = new MockHttpServletRequest();
    AggregationBuilder builder = new AggregationBuilder(req);

    builder.maybeAddByNameAggregation("test");
    assertEquals(0, builder.toList().size());
  }

  @Test
  public void shouldAddByNameAggregationWhenParameterIsAvailable() {
    MockHttpServletRequest req = new MockHttpServletRequest();
    req.addParameter("someString-granularity", "by-name");
    AggregationBuilder builder = new AggregationBuilder(req);

    builder.maybeAddByNameAggregation("someString");
    assertEquals(1, builder.toList().size());
    assertThat(builder.toList().get(0), is(instanceOf(ByNameAggregation.class)));
  }

  @Test
  public void shouldNotAddIntegerAggregationForEmptyRequest() {
    MockHttpServletRequest req = new MockHttpServletRequest();
    AggregationBuilder builder = new AggregationBuilder(req);

    builder.maybeAddIntegerAggregation("test");
    assertEquals(0, builder.toList().size());
  }

  @Test
  public void shouldAddIntegerAggregationWhenParameterIsAvailable() {
    MockHttpServletRequest req = new MockHttpServletRequest();
    req.addParameter("someInt-granularity", "1000s");
    AggregationBuilder builder = new AggregationBuilder(req);

    builder.maybeAddIntegerAggregation("someInt");
    assertEquals(1, builder.toList().size());
    assertThat(builder.toList().get(0), is(instanceOf(IntegerAggregation.class)));

    IntegerAggregation a = (IntegerAggregation) builder.toList().get(0);
    assertEquals("someInt", a.getAttributeName());
    assertEquals(1000, a.getGranularity());
  }

  @Test
  public void shouldBucketInteger() {
    MockHttpServletRequest req = new MockHttpServletRequest();
    req.addParameter("someInt-granularity", "100s");
    AggregationBuilder builder = new AggregationBuilder(req);
    builder.maybeAddIntegerAggregation("someInt");

    IntegerAggregation a = (IntegerAggregation) builder.toList().get(0);
    assertEquals("500", a.bucket("523"));
  }
}
