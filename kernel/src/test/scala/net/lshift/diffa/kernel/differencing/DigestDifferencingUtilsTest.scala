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

package net.lshift.diffa.kernel.differencing

import org.junit.Test
import org.junit.Assert._
import collection.immutable.HashSet
import net.lshift.diffa.kernel.util.DateUtils._
import net.lshift.diffa.kernel.util.Dates._
import net.lshift.diffa.kernel.participants._
import collection.mutable.HashMap
import org.joda.time.DateTime

/**
 * Test cases for the DigestDifferencingUtils object.
 */
class DigestDifferencingUtilsTest {

  def dateRangeConstraint(start:DateTime, end:DateTime) = {
    RangeQueryConstraint("bizDate", Seq(start.toString,end.toString))
  }

  def dateRangeConstraint() = {
    RangeQueryConstraint("bizDate", Seq())
  }

  val unconstrained = EasyConstraints.unconstrainedDate("bizDate")
  val dateOnlyAttrs = Seq("bizDate")

  def resolve(d:Digest) = {
    new HashMap[String,String]
  }

  def bizDateMapper(d: Digest) = {
    HashMap("bizDate" -> d.attributes(0))
  }

  @Test
  def shouldReportNothingOnMatchingEmptyLists {
    val actions = DigestDifferencingUtils.differenceAggregates(Seq(), Seq(), Map(), Seq(unconstrained))
    assertEquals(0, actions.length)
  }

  @Test
  def shouldReportNothingOnMatchingNonEmptyLists {
    val a = Seq(EntityVersion("id1", Seq(JAN_1_2010.toString), JAN_1_2010, "h1"),
                EntityVersion("id2", Seq(JAN_1_2010.toString), JAN_1_2010, "h2"))
    val b = Seq(EntityVersion("id1", Seq(JAN_1_2010.toString), JAN_1_2010, "h1"),
                EntityVersion("id2", Seq(JAN_1_2010.toString), JAN_1_2010, "h2"))

    val actions = DigestDifferencingUtils.differenceEntities(dateOnlyAttrs, a, b, Seq(unconstrained))
    assertEquals(0, actions.length)
  }

  @Test
  def shouldReportNothingOnMatchingNonEmptyListsEvenWhenTheirOrderDiffers {
    val a = Seq(EntityVersion("id2", Seq(JAN_1_2010.toString), JAN_1_2010, "h2"),
                EntityVersion("id1", Seq(JAN_1_2010.toString), JAN_1_2010, "h1"))
    val b = Seq(EntityVersion("id1", Seq(JAN_1_2010.toString), JAN_1_2010, "h1"),
                EntityVersion("id2", Seq(JAN_1_2010.toString), JAN_1_2010, "h2"))

    val actions = DigestDifferencingUtils.differenceEntities(dateOnlyAttrs, a, b, Seq(unconstrained))
    assertEquals(0, actions.length)
  }

  @Test
  def shouldReportMissingIndividualVersionsInFirstList {
    val a = Seq()
    val b = Seq(EntityVersion("id1", Seq(JAN_1_2010.toString), JAN_1_2010, "v1"))

    val actions = DigestDifferencingUtils.differenceEntities(dateOnlyAttrs, a, b, Seq(unconstrained))
    val attributes = Map("bizDate" -> JAN_1_2010.toString())
    assertEquals(HashSet(VersionMismatch("id1", attributes, JAN_1_2010, null, "v1")), HashSet(actions: _*))
  }
  
  @Test
  def shouldReportMissingIndividualVersionsInSecondList {
    val a = Seq(EntityVersion("id1", Seq(JAN_1_2010.toString), JAN_1_2010, "v1"))
    val b = Seq()

    val actions = DigestDifferencingUtils.differenceEntities(dateOnlyAttrs, a, b, Seq(unconstrained))
    val attributes = Map("bizDate" -> JAN_1_2010.toString())
    assertEquals(HashSet(VersionMismatch("id1", attributes, JAN_1_2010, "v1", null)), HashSet(actions: _*))
  }

  @Test
  def shouldReportMismatchedIndividualVersions {
    val a = Seq(EntityVersion("id1", Seq(JAN_1_2010.toString), JAN_1_2010, "v1"))
    val b = Seq(EntityVersion("id1", Seq(JAN_1_2010.toString), JAN_1_2010, "v2"))

    val actions = DigestDifferencingUtils.differenceEntities(dateOnlyAttrs, a, b, Seq(unconstrained))
    val attributes = Map("bizDate" -> JAN_1_2010.toString())
    assertEquals(HashSet(VersionMismatch("id1",  attributes, JAN_1_2010, "v1", "v2")), HashSet(actions: _*))
  }

  @Test
  def shouldRequestIndividualOnMissingDayVersionsInFirstList {
    val a = Seq()
    val b = Seq(AggregateDigest(Seq("2010-07-08"), JUL_8_2010, "v1"))

    val actions = DigestDifferencingUtils.differenceAggregates(a, b, Map("bizDate" -> DailyCategoryFunction), Seq(unconstrained))
    assertEquals(HashSet(EntityQueryAction(Seq(dateRangeConstraint(JUL_8_2010, endOfDay(JUL_8_2010))))), HashSet(actions: _*))
  }

  @Test
  def shouldRequestIndividualOnMissingDayVersionsInSecondList {
    val a = Seq(AggregateDigest(Seq("2010-07-08"), JUL_8_2010, "v1"))
    val b = Seq()

    val actions = DigestDifferencingUtils.differenceAggregates(a, b, Map("bizDate" -> DailyCategoryFunction), Seq(unconstrained))
    assertEquals(HashSet(EntityQueryAction(Seq(dateRangeConstraint(JUL_8_2010, endOfDay(JUL_8_2010))))), HashSet(actions: _*))
  }

  @Test
  def shouldRequestIndividualOnMismatchedDayVersions {
    val a = Seq(AggregateDigest(Seq("2010-07-08"), JUL_8_2010, "v1"))
    val b = Seq(AggregateDigest(Seq("2010-07-08"), JUL_8_2010, "v2"))

    val actions = DigestDifferencingUtils.differenceAggregates(a, b, Map("bizDate" -> DailyCategoryFunction), Seq(unconstrained))
    assertEquals(HashSet(EntityQueryAction(Seq(dateRangeConstraint(JUL_8_2010, endOfDay(JUL_8_2010))))), HashSet(actions: _*))
  }

  @Test
  def shouldRequestIndividualOnMissingMonthVersionsInFirstList {
    val a = Seq()
    val b = Seq(AggregateDigest(Seq("2010-07"), JUL_1_2010, "v1"))

    val actions = DigestDifferencingUtils.differenceAggregates(a, b, Map("bizDate" -> MonthlyCategoryFunction), Seq(unconstrained))
    assertEquals(HashSet(EntityQueryAction(Seq(dateRangeConstraint(JUL_1_2010, endOfDay(JUL_31_2010))))), HashSet(actions: _*))
  }

  @Test
  def shouldRequestIndividualOnMissingMonthVersionsInSecondList {
    val a = Seq(AggregateDigest(Seq("2010-07"), JUL_1_2010, "v1"))
    val b = Seq()

    val actions = DigestDifferencingUtils.differenceAggregates(a, b, Map("bizDate" -> MonthlyCategoryFunction), Seq(unconstrained))
    assertEquals(HashSet(EntityQueryAction(Seq(dateRangeConstraint(JUL_1_2010, endOfDay(JUL_31_2010))))), HashSet(actions: _*))
  }

  @Test
  def shouldRequestDayOnMismatchedMonthVersions {
    val a = Seq(AggregateDigest(Seq("2010-07"), JUL_1_2010, "v1"))
    val b = Seq(AggregateDigest(Seq("2010-07"), JUL_1_2010, "v2"))

    val actions = DigestDifferencingUtils.differenceAggregates(a, b, Map("bizDate" -> MonthlyCategoryFunction), Seq(unconstrained))
    assertEquals(HashSet(AggregateQueryAction(Map("bizDate" -> DailyCategoryFunction), Seq(dateRangeConstraint(JUL_1_2010, endOfDay(JUL_31_2010))))), HashSet(actions: _*))
  }

  @Test
  def shouldRequestIndividualOnMissingYearVersionsInFirstList {
    val a = Seq()
    val b = Seq(AggregateDigest(Seq("2010"), JAN_1_2010, "v1"))

    val actions = DigestDifferencingUtils.differenceAggregates(a, b, Map("bizDate" -> YearlyCategoryFunction), Seq(unconstrained))
    assertEquals(HashSet(EntityQueryAction(Seq(dateRangeConstraint(JAN_1_2010, endOfDay(DEC_31_2010))))), HashSet(actions: _*))
  }

  @Test
  def shouldRequestIndividualOnMissingYearVersionsInSecondList {
    val a = Seq(AggregateDigest(Seq("2010"), JAN_1_2010, "v1"))
    val b = Seq()

    val actions = DigestDifferencingUtils.differenceAggregates(a, b, Map("bizDate" -> YearlyCategoryFunction), Seq(unconstrained))
    assertEquals(HashSet(EntityQueryAction(Seq(dateRangeConstraint(JAN_1_2010, endOfDay(DEC_31_2010))))), HashSet(actions: _*))
  }

  @Test
  def shouldRequestMonthOnMismatchedYearVersions {
    val a = Seq(AggregateDigest(Seq("2010"), JAN_1_2010, "v1"))
    val b = Seq(AggregateDigest(Seq("2010"), JAN_1_2010, "v2"))

    val actions = DigestDifferencingUtils.differenceAggregates(a, b, Map("bizDate" -> YearlyCategoryFunction), Seq(unconstrained))
    assertEquals(HashSet(AggregateQueryAction(Map("bizDate" -> MonthlyCategoryFunction), Seq(dateRangeConstraint(JAN_1_2010, endOfDay(DEC_31_2010))))), HashSet(actions: _*))
  }
}