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

package net.lshift.diffa.kernel.frontend

import org.junit.Test
import org.junit.Assert._
import scala.collection.JavaConversions._
import net.lshift.diffa.kernel.config._

class ConfigValidationTest {

  @Test(expected = classOf[ConfigValidationException])
  def shouldRejectEndpointWithNullContentType = {
    val endpointLog = new EndpointDef(contentType = null)
    endpointLog.validate("")
  }

  @Test(expected = classOf[ConfigValidationException])
  def shouldRejectEndpointWithScanUrlThatIsTooLong = {
    val endpointLog = new EndpointDef(contentType = "", scanUrl = "*" * 1025)
    endpointLog.validate("")
  }

  @Test
  def shouldRejectPairWithScanCronSpecThatIsntACronSpec() {
    val pairDef = PairDef(key = "p", upstreamName = "a", downstreamName = "b", scanCronSpec = "1 2 3")
    val endpoints = Set(EndpointDef(name = "a"), EndpointDef(name = "b"))

    try {
      pairDef.validate("config", endpoints)
      fail("Should have thrown ConfigValidationException")
    } catch {
      case e:ConfigValidationException =>
        assertEquals("config/pair[key=p]: Schedule '1 2 3' is not a valid: Unexpected end of expression.", e.getMessage)
    }
  }

  @Test
  def shouldRejectPairThatUsesAnUpstreamEndpointThatDoesntExist() {
    val pairDef = PairDef(key = "p", upstreamName = "c", downstreamName = "b")
    val endpoints = Set(EndpointDef(name = "a"), EndpointDef(name = "b"))

    try {
      pairDef.validate("config", endpoints)
      fail("Should have thrown ConfigValidationException")
    } catch {
      case e:ConfigValidationException =>
        assertEquals("config/pair[key=p]: Upstream endpoint 'c' is not defined", e.getMessage)
    }
  }

  @Test
  def shouldRejectPairThatUsesADownstreamEndpointThatDoesntExist() {
    val pairDef = PairDef(key = "p", upstreamName = "a", downstreamName = "c")
    val endpoints = Set(EndpointDef(name = "a"), EndpointDef(name = "b"))

    try {
      pairDef.validate("config", endpoints)
      fail("Should have thrown ConfigValidationException")
    } catch {
      case e:ConfigValidationException =>
        assertEquals("config/pair[key=p]: Downstream endpoint 'c' is not defined", e.getMessage)
    }
  }

  @Test
  def shouldRejectViewsWithCategoriesNotPresentOnParent() {
    val endpointDef = EndpointDef(name = "endpointA", contentType = "application/json",
      categories = Map("someString" -> new SetCategoryDescriptor(Set("a", "b"))),
      views = List(EndpointViewDef(name = "invalid",
        categories = Map("otherString" -> new SetCategoryDescriptor(Set("c", "d"))))))

    try {
      endpointDef.validate()
      fail("Should have thrown ConfigValidationException")
    } catch {
      case e:ConfigValidationException =>
        assertEquals("endpoint[name=endpointA]/views[name=invalid]: View category 'otherString' does not derive from an endpoint category", e.getMessage)
    }
  }

  @Test
  def shouldRejectViewsWithCategoriesThatArentWithinBoundsOfParent() {
    val endpointDef = EndpointDef(name = "endpointA", contentType = "application/json",
      categories = Map("someString" -> new SetCategoryDescriptor(Set("a", "b"))),
      views = List(EndpointViewDef(name = "invalid",
        categories = Map("someString" -> new SetCategoryDescriptor(Set("c", "d"))))))

    try {
      endpointDef.validate()
      fail("Should have thrown ConfigValidationException")
    } catch {
      case e:ConfigValidationException =>
        assertEquals("endpoint[name=endpointA]/views[name=invalid]: View category 'someString' (SetCategoryDescriptor{values=[c, d]}) does not refine endpoint category (SetCategoryDescriptor{values=[a, b]})", e.getMessage)
    }
  }

  @Test
  def shouldAcceptAnyPrefixCategoryRefinementsThatArePrefixCategories() {
    val base = new PrefixCategoryDescriptor(5, 12, 1)

    assertTrue(base.isRefinement(new PrefixCategoryDescriptor(2, 12, 1)))
    assertTrue(base.isRefinement(new PrefixCategoryDescriptor(5, 10, 2)))
  }

  @Test
  def shouldRejectAnyPrefixCategoryRefinementsThatAreNotPrefixCategories() {
    val base = new PrefixCategoryDescriptor(5, 12, 1)

    assertFalse(base.isRefinement(new SetCategoryDescriptor(Set("a", "b"))))
    assertFalse(base.isRefinement(new RangeCategoryDescriptor("date", "2011-01-01", "2011-12-31")))
  }

  @Test
  def shouldAcceptSetCategoriesThatAreEqualOrSubset() {
    val base = new SetCategoryDescriptor(Set("a", "b", "c"))

    assertTrue(base.isRefinement(new SetCategoryDescriptor(Set("c", "b", "a"))))
    assertTrue(base.isRefinement(new SetCategoryDescriptor(Set("a", "c"))))
    assertTrue(base.isRefinement(new SetCategoryDescriptor(Set[String]())))
  }

  @Test
  def shouldRejectSetCategoriesThatAreNotEqualOrSubset() {
    val base = new SetCategoryDescriptor(Set("a", "b", "c"))

    assertFalse(base.isRefinement(new SetCategoryDescriptor(Set("c", "b", "a", "d"))))
    assertFalse(base.isRefinement(new SetCategoryDescriptor(Set("a", "d"))))
  }

  @Test
  def shouldRejectAnySetCategoryRefinementsThatAreNotSetCategories() {
    val base = new SetCategoryDescriptor(Set("a", "b", "c"))

    assertFalse(base.isRefinement(new PrefixCategoryDescriptor(5, 12, 1)))
    assertFalse(base.isRefinement(new RangeCategoryDescriptor("date", "2011-01-01", "2011-12-31")))
  }

  @Test
  def shouldAcceptDateRangeCategoriesThatAreEqualOrSubset() {
    val base = new RangeCategoryDescriptor("date", "2011-01-01", "2011-12-31")
    val unboundedBase = new RangeCategoryDescriptor("date", null, null)

    assertTrue(base.isRefinement(new RangeCategoryDescriptor("date", "2011-01-01", "2011-12-31")))
    assertTrue(base.isRefinement(new RangeCategoryDescriptor("date", "2011-05-05", "2011-11-01")))

    assertTrue(unboundedBase.isRefinement(new RangeCategoryDescriptor("date", "2011-05-05", "2011-11-01")))
    assertTrue(unboundedBase.isRefinement(new RangeCategoryDescriptor("date", "2011-05-05", null)))
    assertTrue(unboundedBase.isRefinement(new RangeCategoryDescriptor("date", null, "2014-11-01")))
    assertTrue(unboundedBase.isRefinement(new RangeCategoryDescriptor("date", null, null)))
  }

  @Test
  def shouldRejectDateRangeCategoriesThatAreNotWithinOuterRange() {
    val base = new RangeCategoryDescriptor("date", "2011-01-01", "2011-12-31")

    assertFalse(base.isRefinement(new RangeCategoryDescriptor("date", "2010-12-31", "2011-05-05")))
    assertFalse(base.isRefinement(new RangeCategoryDescriptor("date", "2011-03-03", "2012-01-01")))
  }

  @Test
  def shouldRejectAnyDateRangeCategoryRefinementsThatAreNotDateRangeCategories() {
    val base = new RangeCategoryDescriptor("date", "2011-01-01", "2011-12-31")

    assertFalse(base.isRefinement(new SetCategoryDescriptor(Set("c", "b", "a", "d"))))
    assertFalse(base.isRefinement(new PrefixCategoryDescriptor(5, 12, 1)))
    assertFalse(base.isRefinement(new RangeCategoryDescriptor("datetime", "2011-01-01T00:00:00.000Z", "2011-12-31T23:59:59.999Z")))
    assertFalse(base.isRefinement(new RangeCategoryDescriptor("integer", "52", "104")))
  }

  @Test
  def shouldAcceptTimeRangeCategoriesThatAreEqualOrSubset() {
    val base = new RangeCategoryDescriptor("datetime", "2011-01-01T12:52:12.123Z", "2011-12-31T05:12:13.876Z")
    val unboundedBase = new RangeCategoryDescriptor("datetime", null, null)

    assertTrue(base.isRefinement(new RangeCategoryDescriptor("datetime", "2011-01-01T12:52:12.123Z", "2011-12-31T05:12:13.876Z")))
    assertTrue(base.isRefinement(new RangeCategoryDescriptor("datetime", "2011-05-05T01:02:03.000Z", "2011-11-01T12:13:14.123Z")))

    assertTrue(unboundedBase.isRefinement(new RangeCategoryDescriptor("datetime", "2011-05-05T12:52:12.123Z", "2011-11-01T12:52:12.123Z")))
    assertTrue(unboundedBase.isRefinement(new RangeCategoryDescriptor("datetime", "2011-05-05T12:52:12.123Z", null)))
    assertTrue(unboundedBase.isRefinement(new RangeCategoryDescriptor("datetime", null, "2014-11-01T12:52:12.123Z")))
    assertTrue(unboundedBase.isRefinement(new RangeCategoryDescriptor("datetime", null, null)))
  }

  @Test
  def shouldRejectTimeRangeCategoriesThatAreNotWithinOuterRange() {
    val base = new RangeCategoryDescriptor("datetime", "2011-01-01T12:52:12.123Z", "2011-12-31T23:59:59.999Z")

    assertFalse(base.isRefinement(new RangeCategoryDescriptor("datetime", "2011-01-01T12:52:12.122Z", "2011-05-05T12:52:12.123Z")))
    assertFalse(base.isRefinement(new RangeCategoryDescriptor("datetime", "2011-03-03T12:52:12.123Z", "2012-01-01T00:00:00.000Z")))
  }

  @Test
  def shouldRejectAnyTimeRangeCategoryRefinementsThatAreNotTimeRangeCategories() {
    val base = new RangeCategoryDescriptor("datetime", "2011-01-01T00:00:00.000Z", "2011-12-31T00:00:00.000Z")

    assertFalse(base.isRefinement(new SetCategoryDescriptor(Set("c", "b", "a", "d"))))
    assertFalse(base.isRefinement(new PrefixCategoryDescriptor(5, 12, 1)))
    assertFalse(base.isRefinement(new RangeCategoryDescriptor("date", "2011-01-01", "2011-12-31")))
    assertFalse(base.isRefinement(new RangeCategoryDescriptor("integer", "52", "104")))
  }

  @Test
  def shouldAcceptIntegerRangeCategoriesThatAreEqualOrSubset() {
    val base = new RangeCategoryDescriptor("integer", "52", "104")
    val unboundedBase = new RangeCategoryDescriptor("integer", null, null)

    assertTrue(base.isRefinement(new RangeCategoryDescriptor("integer", "52", "104")))
    assertTrue(base.isRefinement(new RangeCategoryDescriptor("integer", "58", "101")))

    assertTrue(unboundedBase.isRefinement(new RangeCategoryDescriptor("integer", "1", "1200")))
    assertTrue(unboundedBase.isRefinement(new RangeCategoryDescriptor("integer", "57", null)))
    assertTrue(unboundedBase.isRefinement(new RangeCategoryDescriptor("integer", null, "57")))
    assertTrue(unboundedBase.isRefinement(new RangeCategoryDescriptor("integer", null, null)))
  }

  @Test
  def shouldRejectIntegerRangeCategoriesThatAreNotWithinOuterRange() {
    val base = new RangeCategoryDescriptor("integer", "52", "104")

    assertFalse(base.isRefinement(new RangeCategoryDescriptor("integer", "51", "108")))
    assertFalse(base.isRefinement(new RangeCategoryDescriptor("integer", "56", "105")))
  }

  @Test
  def shouldRejectAnyIntegerRangeCategoryRefinementsThatAreNotIntegerRangeCategories() {
    val base = new RangeCategoryDescriptor("integer", "52", "104")

    assertFalse(base.isRefinement(new SetCategoryDescriptor(Set("c", "b", "a", "d"))))
    assertFalse(base.isRefinement(new PrefixCategoryDescriptor(5, 12, 1)))
    assertFalse(base.isRefinement(new RangeCategoryDescriptor("date", "2011-01-01", "2011-12-31")))
    assertFalse(base.isRefinement(new RangeCategoryDescriptor("datetime", "2011-01-01T00:00:00.000Z", "2011-12-31T00:00:00.000Z")))
  }

  @Test
  def shouldAcceptPairViewThatUsesViewsThatExistOnBothEndpoints() {
    val pairDef = PairDef(key = "p", upstreamName = "a", downstreamName = "b", views = List(PairViewDef("abc")))
    val endpoints = Set(
      EndpointDef(name = "a", views = List(EndpointViewDef(name = "abc"))),
      EndpointDef(name = "b", views = List(EndpointViewDef(name = "abc"))))

    pairDef.validate("config", endpoints)
  }

  @Test
  def shouldRejectPairViewThatUsesAnUpstreamViewThatDoesntExist() {
    val pairDef = PairDef(key = "p", upstreamName = "a", downstreamName = "b", views = List(PairViewDef("abc")))
    val endpoints = Set(EndpointDef(name = "a"), EndpointDef(name = "b", views = List(EndpointViewDef(name = "abc"))))

    try {
      pairDef.validate("config", endpoints)
      fail("Should have thrown ConfigValidationException")
    } catch {
      case e:ConfigValidationException =>
        assertEquals("config/pair[key=p]/views[name=abc]: The upstream endpoint does not define the view 'abc'", e.getMessage)
    }
  }

  @Test
  def shouldRejectPairViewThatUsesADownstreamViewThatDoesntExist() {
    val pairDef = PairDef(key = "p", upstreamName = "a", downstreamName = "b", views = List(PairViewDef("abc")))
    val endpoints = Set(EndpointDef(name = "a", views = List(EndpointViewDef(name = "abc"))), EndpointDef(name = "b"))

    try {
      pairDef.validate("config", endpoints)
      fail("Should have thrown ConfigValidationException")
    } catch {
      case e:ConfigValidationException =>
        assertEquals("config/pair[key=p]/views[name=abc]: The downstream endpoint does not define the view 'abc'", e.getMessage)
    }
  }

  @Test
  def shouldAcceptPairViewThatUsesValidCronExpression() {
    val pairDef = PairDef(key = "p", upstreamName = "a", downstreamName = "b",
      views = List(PairViewDef("abc", scanCronSpec = "0 * * * * ?")))
    val endpoints = Set(
      EndpointDef(name = "a", views = List(EndpointViewDef(name = "abc"))),
      EndpointDef(name = "b", views = List(EndpointViewDef(name = "abc"))))

    pairDef.validate("config", endpoints)
  }

  @Test
  def shouldRejectPairViewThatUsesInvalidCronExpression() {
    val pairDef = PairDef(key = "p", upstreamName = "a", downstreamName = "b",
      views = List(PairViewDef("abc", scanCronSpec = "1 2 3")))
    val endpoints = Set(
      EndpointDef(name = "a", views = List(EndpointViewDef(name = "abc"))),
      EndpointDef(name = "b", views = List(EndpointViewDef(name = "abc"))))

    try {
      pairDef.validate("config", endpoints)
      fail("Should have thrown ConfigValidationException")
    } catch {
      case e:ConfigValidationException =>
        assertEquals("config/pair[key=p]/views[name=abc]: Schedule '1 2 3' is not a valid: Unexpected end of expression.", e.getMessage)
    }
  }

  @Test
  def shouldAcceptReportWithValidReportType() {
    val reportDef = PairReportDef(name = "Process Differences", pair ="p", reportType = "differences",
                                  target = "http://someapp.com/handle_report")
    reportDef.validate("config")
  }

  @Test
  def shouldRejectReportWithInvalidReportType() {
    val reportDef = PairReportDef(name = "Process Differences", pair ="p", reportType = "blah-blah",
                                  target = "http://someapp.com/handle_report")
    try {
      reportDef.validate("config")
      fail("Should have thrown ConfigValidationException")
    } catch {
      case e:ConfigValidationException =>
        assertEquals("config/pair[key=p]/report[name=Process Differences]: Invalid report type: blah-blah", e.getMessage)
    }
  }

  @Test
  def shouldRejectReportWithMissingTarget() {
    val reportDef = PairReportDef(name = "Process Differences", pair ="p", reportType = "differences")
    try {
      reportDef.validate("config")
      fail("Should have thrown ConfigValidationException")
    } catch {
      case e:ConfigValidationException =>
        assertEquals("config/pair[key=p]/report[name=Process Differences]: Missing target", e.getMessage)
    }
  }

  @Test
  def shouldRejectReportWithInvalidTarget() {
    val reportDef = PairReportDef(name = "Process Differences", pair ="p", reportType = "differences",
                                  target = "random-target")
    try {
      reportDef.validate("config")
      fail("Should have thrown ConfigValidationException")
    } catch {
      case e:ConfigValidationException =>
        assertEquals("config/pair[key=p]/report[name=Process Differences]: Invalid target (not a URL): random-target", e.getMessage)
    }
  }
}