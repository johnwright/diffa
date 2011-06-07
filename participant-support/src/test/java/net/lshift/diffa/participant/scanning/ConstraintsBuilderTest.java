package net.lshift.diffa.participant.scanning;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.junit.Test;
import org.springframework.mock.web.MockHttpServletRequest;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Test cases for the ConstraintsBuilder.
 */
public class ConstraintsBuilderTest {
  @Test
  public void shouldNotAddDateRangeConstraintForEmptyRequest() {
    MockHttpServletRequest req = new MockHttpServletRequest();
    ConstraintsBuilder builder = new ConstraintsBuilder(req);

    builder.maybeAddDateRangeConstraint("test");
    assertEquals(0, builder.toList().size());
  }

  @Test
  public void shouldNotAddTimeRangeConstraintForEmptyRequest() {
    MockHttpServletRequest req = new MockHttpServletRequest();
    ConstraintsBuilder builder = new ConstraintsBuilder(req);

    builder.maybeAddTimeRangeConstraint("test");
    assertEquals(0, builder.toList().size());
  }

  @Test
  public void shouldNotAddSetConstraintForEmptyRequest() {
    MockHttpServletRequest req = new MockHttpServletRequest();
    ConstraintsBuilder builder = new ConstraintsBuilder(req);

    builder.maybeAddSetConstraint("test");
    assertEquals(0, builder.toList().size());
  }

  @Test
  public void shouldAddDateRangeConstraintWhenBothStartAndEndArePresent() {
    MockHttpServletRequest req = new MockHttpServletRequest();
    req.addParameter("bizDate-start", "2011-06-01");
    req.addParameter("bizDate-end", "2011-06-30");
    ConstraintsBuilder builder = new ConstraintsBuilder(req);

    builder.maybeAddDateRangeConstraint("bizDate");
    assertEquals(1, builder.toList().size());
    assertThat(builder.toList().get(0), is(instanceOf(DateRangeConstraint.class)));

    DateRangeConstraint c = (DateRangeConstraint) builder.toList().get(0);
    assertEquals(new LocalDate(2011, 6, 1), c.getStart());
    assertEquals(new LocalDate(2011, 6, 30), c.getEnd());
    assertEquals("bizDate",c.getAttributeName());
  }

  @Test
  public void shouldAddTimeRangeConstraintWhenBothStartAndEndArePresent() {
    MockHttpServletRequest req = new MockHttpServletRequest();
    req.addParameter("createTime-start", "2011-06-06T12:00:00.000Z");
    req.addParameter("createTime-end", "2011-06-06T16:00:00.000Z");
    ConstraintsBuilder builder = new ConstraintsBuilder(req);

    builder.maybeAddTimeRangeConstraint("createTime");
    assertEquals(1, builder.toList().size());
    assertThat(builder.toList().get(0), is(instanceOf(TimeRangeConstraint.class)));

    TimeRangeConstraint c = (TimeRangeConstraint) builder.toList().get(0);
    assertEquals(new DateTime(2011, 6, 6, 12, 0, 0, 0, DateTimeZone.UTC), c.getStart());
    assertEquals(new DateTime(2011, 6, 6, 16, 0, 0, 0, DateTimeZone.UTC), c.getEnd());
  }

  @Test
  public void shouldAddSetConstraintWhenSingleValueIsPresent() {
    MockHttpServletRequest req = new MockHttpServletRequest();
    req.addParameter("someString", "a");
    ConstraintsBuilder builder = new ConstraintsBuilder(req);

    builder.maybeAddSetConstraint("someString");
    assertEquals(1, builder.toList().size());
    assertThat(builder.toList().get(0), is(instanceOf(SetConstraint.class)));

    SetConstraint c = (SetConstraint) builder.toList().get(0);

    Set<String> expected = new HashSet<String>();
    expected.add("a");
    assertEquals(expected, c.getValues());
  }

  @Test
  public void shouldAddSetConstraintWhenMultipleValuesArePresent() {
    MockHttpServletRequest req = new MockHttpServletRequest();
    req.addParameter("someString", "a");
    req.addParameter("someString", "b");
    req.addParameter("someString", "c");
    ConstraintsBuilder builder = new ConstraintsBuilder(req);

    builder.maybeAddSetConstraint("someString");
    assertEquals(1, builder.toList().size());
    assertThat(builder.toList().get(0), is(instanceOf(SetConstraint.class)));

    SetConstraint c = (SetConstraint) builder.toList().get(0);
    Set<String> expected = new HashSet<String>();
    expected.add("a");
    expected.add("b");
    expected.add("c");
    assertEquals(expected, c.getValues());
  }
}
