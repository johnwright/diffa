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
  public void shouldAddIntegerConstraintWhenBothStartAndEndArePresent() {
    MockHttpServletRequest req = new MockHttpServletRequest();
    req.addParameter("someInt-start", "5");
    req.addParameter("someInt-end", "50");
    ConstraintsBuilder builder = new ConstraintsBuilder(req);

    builder.maybeAddIntegerRangeConstraint("someInt");
    assertEquals(1, builder.toList().size());
    assertThat(builder.toList().get(0), is(instanceOf(IntegerRangeConstraint.class)));

    IntegerRangeConstraint c = (IntegerRangeConstraint) builder.toList().get(0);
    assertEquals(new Integer(5), c.getStart());
    assertEquals(new Integer(50), c.getEnd());
  }

  @Test
  public void shouldAddStringPrefixConstraintWhenPrefixIsPresent() {
    MockHttpServletRequest req = new MockHttpServletRequest();
    req.addParameter("someString-prefix", "ab");
    ConstraintsBuilder builder = new ConstraintsBuilder(req);

    builder.maybeAddStringPrefixConstraint("someString");
    assertEquals(1, builder.toList().size());
    assertThat(builder.toList().get(0), is(instanceOf(StringPrefixConstraint.class)));

    StringPrefixConstraint c = (StringPrefixConstraint) builder.toList().get(0);
    assertEquals("ab", c.getPrefix());
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
