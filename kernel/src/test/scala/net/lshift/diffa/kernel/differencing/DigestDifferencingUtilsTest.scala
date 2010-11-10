/**
 * Copyright (C) 2010 LShift Ltd.
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

/**
 * Test cases for the DigestDifferencingUtils object.
 */
class DigestDifferencingUtilsTest {

  val __attributes = new HashMap[String,String]
  
  // TODO [#2] factor these crufty granularities out

  val in = DateCategoryFunction()
  val day = DateCategoryFunction()
  val month = DateCategoryFunction()
  val year = DateCategoryFunction()


  val IndividualGranularity = Seq(DateRangeConstraint(null, null, in))
  val DayGranularity = Seq(DateRangeConstraint(null, null, day))
  val MonthGranularity = Seq(DateRangeConstraint(null, null, month))
  val YearGranularity = Seq(DateRangeConstraint(null, null, year))

  def resolve(d:Digest) = {
    new HashMap[String,String]
  }

  def bizDateMapper(d: Digest) = {
    HashMap("bizDate" -> d.attributes(0))
  }

  @Test
  def shouldReportNothingOnMatchingEmptyLists {
    val actions = DigestDifferencingUtils.differenceAggregates(Seq(), Seq(), resolve, IndividualGranularity)
    assertEquals(0, actions.length)
  }

  @Test
  def shouldReportNothingOnMatchingNonEmptyLists {
    val a = Seq(EntityVersion("id1", Seq(JAN_1_2010.toString), JAN_1_2010, "h1"),
                EntityVersion("id2", Seq(JAN_1_2010.toString), JAN_1_2010, "h2"))
    //val a = Seq(Digest("id1", JAN_1_2010, JAN_1_2010, "h1"), Digest("id2", JAN_1_2010, JAN_1_2010, "h2"))
    val b = Seq(EntityVersion("id1", Seq(JAN_1_2010.toString), JAN_1_2010, "h1"),
                EntityVersion("id2", Seq(JAN_1_2010.toString), JAN_1_2010, "h2"))
    //val b = Seq(Digest("id1", JAN_1_2010, JAN_1_2010, "h1"), Digest("id2", JAN_1_2010, JAN_1_2010, "h2"))

    val actions = DigestDifferencingUtils.differenceEntities(a, b, resolve, IndividualGranularity)
    assertEquals(0, actions.length)
  }

  @Test
  def shouldReportNothingOnMatchingNonEmptyListsEvenWhenTheirOrderDiffers {
    val a = Seq(EntityVersion("id2", Seq(JAN_1_2010.toString), JAN_1_2010, "h2"),
                EntityVersion("id1", Seq(JAN_1_2010.toString), JAN_1_2010, "h1"))
    //val a = Seq(Digest("id2", JAN_1_2010, JAN_1_2010, "h2"), Digest("id1", JAN_1_2010, JAN_1_2010, "h1"))
    val b = Seq(EntityVersion("id1", Seq(JAN_1_2010.toString), JAN_1_2010, "h1"),
                EntityVersion("id2", Seq(JAN_1_2010.toString), JAN_1_2010, "h2"))
    //val b = Seq(Digest("id1", JAN_1_2010, JAN_1_2010, "h1"), Digest("id2", JAN_1_2010, JAN_1_2010, "h2"))

    val actions = DigestDifferencingUtils.differenceEntities(a, b, resolve, IndividualGranularity)
    assertEquals(0, actions.length)
  }

  @Test
  def shouldReportMissingIndividualVersionsInFirstList {
    val a = Seq()
    val b = Seq(EntityVersion("id1", Seq(JAN_1_2010.toString), JAN_1_2010, "v1"))

    val actions = DigestDifferencingUtils.differenceEntities(a, b, bizDateMapper, IndividualGranularity)
    val attributes = HashMap("bizDate" -> JAN_1_2010.toString())
    assertEquals(HashSet(VersionMismatch("id1", attributes, JAN_1_2010, null, "v1")), HashSet(actions: _*))
  }
  
  @Test
  def shouldReportMissingIndividualVersionsInSecondList {
    val a = Seq(EntityVersion("id1", Seq(JAN_1_2010.toString), JAN_1_2010, "v1"))
    val b = Seq()

    val actions = DigestDifferencingUtils.differenceEntities(a, b, bizDateMapper, IndividualGranularity)
    val attributes = HashMap("bizDate" -> JAN_1_2010.toString())
    assertEquals(HashSet(VersionMismatch("id1", attributes, JAN_1_2010, "v1", null)), HashSet(actions: _*))
  }

  @Test
  def shouldReportMismatchedIndividualVersions {
    val a = Seq(EntityVersion("id1", Seq(JAN_1_2010.toString), JAN_1_2010, "v1"))
    //val a = Seq(Digest("id1", JAN_1_2010, JAN_1_2010, "v1"))
    val b = Seq(EntityVersion("id1", Seq(JAN_1_2010.toString), JAN_1_2010, "v2"))
    //val b = Seq(Digest("id1", JAN_1_2010, JAN_1_2010, "v2"))

    val actions = DigestDifferencingUtils.differenceEntities(a, b, resolve, IndividualGranularity)
    assertEquals(HashSet(VersionMismatch("id1",  __attributes, JAN_1_2010, "v1", "v2")), HashSet(actions: _*))
    //assertEquals(HashSet(VersionMismatch("id1", JAN_1_2010, JAN_1_2010, "v1", "v2")), HashSet(actions: _*))
  }

  @Test
  def shouldRequestIndividualOnMissingDayVersionsInFirstList {
    val a = Seq()
    val b = Seq(AggregateDigest(Seq("2010-07-08"), JUL_8_2010, "v1"))
    //val b = Seq(Digest("2010-07-08", JUL_8_2010, JUL_8_2010, "v1"))

    val actions = DigestDifferencingUtils.differenceAggregates(a, b, resolve, DayGranularity)
    assertEquals(HashSet(EntityQueryAction(DateRangeConstraint(JUL_8_2010, endOfDay(JUL_8_2010), in))), HashSet(actions: _*))
    //assertEquals(HashSet(QueryAction(JUL_8_2010, endOfDay(JUL_8_2010), IndividualGranularity)), HashSet(actions: _*))
  }

  @Test
  def shouldRequestIndividualOnMissingDayVersionsInSecondList {
    val a = Seq(AggregateDigest(Seq("2010-07-08"), JUL_8_2010, "v1"))
    //val a = Seq(Digest("2010-07-08", JUL_8_2010, JUL_8_2010, "v1"))
    val b = Seq()

    val actions = DigestDifferencingUtils.differenceAggregates(a, b, resolve, DayGranularity)
    assertEquals(HashSet(EntityQueryAction(DateRangeConstraint(JUL_8_2010, endOfDay(JUL_8_2010), in))), HashSet(actions: _*))
    //assertEquals(HashSet(QueryAction(JUL_8_2010, endOfDay(JUL_8_2010), IndividualGranularity)), HashSet(actions: _*))
  }

  @Test
  def shouldRequestIndividualOnMismatchedDayVersions {
    val a = Seq(AggregateDigest(Seq("2010-07-08"), JUL_8_2010, "v1"))
    //val a = Seq(Digest("2010-07-08", JUL_8_2010, JUL_8_2010, "v1"))
    val b = Seq(AggregateDigest(Seq("2010-07-08"), JUL_8_2010, "v2"))
    //val b = Seq(Digest("2010-07-08", JUL_8_2010, JUL_8_2010, "v2"))

    val actions = DigestDifferencingUtils.differenceAggregates(a, b, resolve, DayGranularity)
    assertEquals(HashSet(EntityQueryAction(DateRangeConstraint(JUL_8_2010, endOfDay(JUL_8_2010), in))), HashSet(actions: _*))
    //assertEquals(HashSet(QueryAction(JUL_8_2010, endOfDay(JUL_8_2010), IndividualGranularity)), HashSet(actions: _*))
  }

  @Test
  def shouldRequestIndividualOnMissingMonthVersionsInFirstList {
    val a = Seq()
    val b = Seq(AggregateDigest(Seq("2010-07"), JUL_1_2010, "v1"))
    //val b = Seq(Digest("2010-07", JUL_1_2010, JUL_1_2010, "v1"))

    val actions = DigestDifferencingUtils.differenceAggregates(a, b, resolve, MonthGranularity)
    assertEquals(HashSet(EntityQueryAction(DateRangeConstraint(JUL_1_2010, endOfDay(JUL_31_2010), in))), HashSet(actions: _*))
    //assertEquals(HashSet(QueryAction(JUL_1_2010, endOfDay(JUL_31_2010), IndividualGranularity)), HashSet(actions: _*))
  }

  @Test
  def shouldRequestIndividualOnMissingMonthVersionsInSecondList {
    val a = Seq(AggregateDigest(Seq("2010-07"), JUL_1_2010, "v1"))
    //val a = Seq(Digest("2010-07", JUL_1_2010, JUL_1_2010, "v1"))
    val b = Seq()

    val actions = DigestDifferencingUtils.differenceAggregates(a, b, resolve, MonthGranularity)
    //assertEquals(HashSet(QueryAction(JUL_1_2010, endOfDay(JUL_31_2010), IndividualGranularity)), HashSet(actions: _*))
    assertEquals(HashSet(EntityQueryAction(DateRangeConstraint(JUL_1_2010, endOfDay(JUL_31_2010), in))), HashSet(actions: _*))
  }

  @Test
  def shouldRequestDayOnMismatchedMonthVersions {
    val a = Seq(AggregateDigest(Seq("2010-07"), JUL_1_2010, "v1"))
    //val a = Seq(Digest("2010-07", JUL_1_2010, JUL_1_2010, "v1"))
    val b = Seq(AggregateDigest(Seq("2010-07"), JUL_1_2010, "v2"))
    //val b = Seq(Digest("2010-07", JUL_1_2010, JUL_1_2010, "v2"))

    val actions = DigestDifferencingUtils.differenceAggregates(a, b, resolve, MonthGranularity)
    assertEquals(HashSet(AggregateQueryAction(DateRangeConstraint(JUL_1_2010, endOfDay(JUL_31_2010), day))), HashSet(actions: _*))
    //assertEquals(HashSet(QueryAction(JUL_1_2010, endOfDay(JUL_31_2010), DayGranularity)), HashSet(actions: _*))
  }

  @Test
  def shouldRequestIndividualOnMissingYearVersionsInFirstList {
    val a = Seq()
    val b = Seq(AggregateDigest(Seq("2010"), JAN_1_2010, "v1"))
    //val b = Seq(Digest("2010", JAN_1_2010, JAN_1_2010, "v1"))

    val actions = DigestDifferencingUtils.differenceAggregates(a, b, resolve, YearGranularity)
    assertEquals(HashSet(EntityQueryAction(DateRangeConstraint(JAN_1_2010, endOfDay(DEC_31_2010), in))), HashSet(actions: _*))
    //assertEquals(HashSet(QueryAction(JAN_1_2010, endOfDay(DEC_31_2010), IndividualGranularity)), HashSet(actions: _*))
  }

  @Test
  def shouldRequestIndividualOnMissingYearVersionsInSecondList {
    //val a = Seq(Digest("2010", JAN_1_2010, JAN_1_2010, "v1"))
    val a = Seq(AggregateDigest(Seq("2010"), JAN_1_2010, "v1"))
    val b = Seq()

    val actions = DigestDifferencingUtils.differenceAggregates(a, b, resolve, YearGranularity)
    assertEquals(HashSet(EntityQueryAction(DateRangeConstraint(JAN_1_2010, endOfDay(DEC_31_2010), in))), HashSet(actions: _*))
    //assertEquals(HashSet(QueryAction(JAN_1_2010, endOfDay(DEC_31_2010), IndividualGranularity)), HashSet(actions: _*))
  }

  @Test
  def shouldRequestMonthOnMismatchedYearVersions {
    val a = Seq(AggregateDigest(Seq("2010"), JAN_1_2010, "v1"))
    //val a = Seq(Digest("2010", JAN_1_2010, JAN_1_2010, "v1"))
    val b = Seq(AggregateDigest(Seq("2010"), JAN_1_2010, "v2"))
    //val b = Seq(Digest("2010", JAN_1_2010, JAN_1_2010, "v2"))

    val actions = DigestDifferencingUtils.differenceAggregates(a, b, resolve, YearGranularity)
    assertEquals(HashSet(AggregateQueryAction(DateRangeConstraint(JAN_1_2010, endOfDay(DEC_31_2010), month))), HashSet(actions: _*))
    //assertEquals(HashSet(QueryAction(JAN_1_2010, endOfDay(DEC_31_2010), MonthGranularity)), HashSet(actions: _*))
  }
}