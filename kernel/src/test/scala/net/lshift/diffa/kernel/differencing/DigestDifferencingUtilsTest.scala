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
import net.lshift.diffa.kernel.util.FullDateTimes._
import net.lshift.diffa.kernel.participants._
import collection.mutable.HashMap
import org.joda.time.DateTime
import net.lshift.diffa.kernel.config.RangeCategoryDescriptor
import net.lshift.diffa.participant.scanning.ScanResultEntry
import scala.collection.JavaConversions._

/**
 * Test cases for the DigestDifferencingUtils object.
 */
class DigestDifferencingUtilsTest {

  def dateTimeRangeConstraint(start:DateTime, end:DateTime) = DateTimeRangeConstraint("bizDateTime", start, end)

  val unconstrained = EasyConstraints.unconstrainedDateTime("bizDateTime")
  val dateTimeCategoryDescriptor = new RangeCategoryDescriptor("datetime")
  val dateTimeOnlyAttrs = Map("bizDateTime" -> dateTimeCategoryDescriptor)

  def resolve(d:Digest) = {
    new HashMap[String,String]
  }

  def bizDateMapper(d: Digest) = {
    HashMap("bizDateTime" -> d.attributes(0))
  }

  @Test
  def shouldReportNothingOnMatchingEmptyLists {
    val actions = DigestDifferencingUtils.differenceAggregates(Seq(), Seq(), Seq(), Seq(unconstrained))
    assertEquals(0, actions.length)
  }

  @Test
  def shouldThrowUsefulExceptionOnBadInputData {
    val a = Seq(ScanResultEntry.forAggregate("v1", Map("bizDateTime" -> "2010")))
    val b = Seq(ScanResultEntry.forAggregate("v2", Map("bizDateTime" -> "2010-07")))

    try {
      DigestDifferencingUtils.differenceAggregates(a, b, Seq(MonthlyCategoryFunction("bizDateTime")), Seq(unconstrained))
      fail("Should have thrown Exception")
    } catch {
      case ex =>
        assertEquals(
          "Invalid format: \"2010\" is too short { bucketing = Map(bizDateTime -> MonthlyCategoryFunction), constraints = List(UnboundedRangeQueryConstraint(bizDateTime)), partitions = Map(bizDateTime -> 2010), results = ListBuffer() }",
          ex.getMessage)
    }
  }

  @Test
  def shouldReportNothingOnMatchingNonEmptyLists {
    val a = Seq(ScanResultEntry.forEntity("id1", "h1", JAN_1_2010, Map("bizDateTime" -> JAN_1_2010.toString())),
                ScanResultEntry.forEntity("id2", "h2", JAN_1_2010, Map("bizDateTime" -> JAN_1_2010.toString())))
    val b = Seq(ScanResultEntry.forEntity("id1", "h1", JAN_1_2010, Map("bizDateTime" -> JAN_1_2010.toString())),
                ScanResultEntry.forEntity("id2", "h2", JAN_1_2010, Map("bizDateTime" -> JAN_1_2010.toString())))

    val actions = DigestDifferencingUtils.differenceEntities(dateTimeOnlyAttrs, a, b, Seq(unconstrained))
    assertEquals(0, actions.length)
  }

  @Test
  def shouldReportNothingOnMatchingNonEmptyListsEvenWhenTheirOrderDiffers {
    val a = Seq(ScanResultEntry.forEntity("id2", "h2", JAN_1_2010, Map("bizDateTime" -> JAN_1_2010.toString())),
                ScanResultEntry.forEntity("id1", "h1", JAN_1_2010, Map("bizDateTime" -> JAN_1_2010.toString())))
    val b = Seq(ScanResultEntry.forEntity("id1", "h1", JAN_1_2010, Map("bizDateTime" -> JAN_1_2010.toString())),
                ScanResultEntry.forEntity("id2", "h2", JAN_1_2010, Map("bizDateTime" -> JAN_1_2010.toString())))

    val actions = DigestDifferencingUtils.differenceEntities(dateTimeOnlyAttrs, a, b, Seq(unconstrained))
    assertEquals(0, actions.length)
  }

  @Test
  def shouldReportMissingIndividualVersionsInFirstList {
    val a = Seq()
    val b = Seq(ScanResultEntry.forEntity("id1", "v1", JAN_1_2010, Map("bizDateTime" -> JAN_1_2010.toString())))

    val actions = DigestDifferencingUtils.differenceEntities(dateTimeOnlyAttrs, a, b, Seq(unconstrained))
    val attributes = Map("bizDateTime" -> DateTimeAttribute(JAN_1_2010))
    assertEquals(HashSet(VersionMismatch("id1", attributes, JAN_1_2010, null, "v1")), HashSet(actions: _*))
  }
  
  @Test
  def shouldReportMissingIndividualVersionsInSecondList {
    val a = Seq(ScanResultEntry.forEntity("id1", "v1", JAN_1_2010, Map("bizDateTime" -> JAN_1_2010.toString())))
    val b = Seq()

    val actions = DigestDifferencingUtils.differenceEntities(dateTimeOnlyAttrs, a, b, Seq(unconstrained))
    val attributes = Map("bizDateTime" -> DateTimeAttribute(JAN_1_2010))
    assertEquals(HashSet(VersionMismatch("id1", attributes, JAN_1_2010, "v1", null)), HashSet(actions: _*))
  }

  @Test
  def shouldReportMismatchedIndividualVersions {
    val a = Seq(ScanResultEntry.forEntity("id1", "v1", JAN_1_2010, Map("bizDateTime" -> JAN_1_2010.toString())))
    val b = Seq(ScanResultEntry.forEntity("id1", "v2", JAN_1_2010, Map("bizDateTime" -> JAN_1_2010.toString())))

    val actions = DigestDifferencingUtils.differenceEntities(dateTimeOnlyAttrs, a, b, Seq(unconstrained))
    val attributes = Map("bizDateTime" -> DateTimeAttribute(JAN_1_2010))
    assertEquals(HashSet(VersionMismatch("id1",  attributes, JAN_1_2010, "v1", "v2")), HashSet(actions: _*))
  }

  @Test
  def shouldRequestIndividualOnMissingDayVersionsInFirstList {
    val a = Seq()
    val b = Seq(ScanResultEntry.forAggregate("v1", Map("bizDateTime" -> "2010-07-08")))

    val actions = DigestDifferencingUtils.differenceAggregates(a, b, Seq(DailyCategoryFunction("bizDateTime")), Seq(unconstrained))
    assertEquals(HashSet(EntityQueryAction(Seq(dateTimeRangeConstraint(JUL_8_2010, endOfDay(JUL_8_2010))))), HashSet(actions: _*))
  }

  @Test
  def shouldRequestIndividualOnMissingDayVersionsInSecondList {
    val a = Seq(ScanResultEntry.forAggregate("v1", Map("bizDateTime" -> "2010-07-08")))
    val b = Seq()

    val actions = DigestDifferencingUtils.differenceAggregates(a, b, Seq(DailyCategoryFunction("bizDateTime")), Seq(unconstrained))
    assertEquals(HashSet(EntityQueryAction(Seq(dateTimeRangeConstraint(JUL_8_2010, endOfDay(JUL_8_2010))))), HashSet(actions: _*))
  }

  @Test
  def shouldRequestIndividualOnMismatchedDayVersions {
    val a = Seq(ScanResultEntry.forAggregate("v1", Map("bizDateTime" -> "2010-07-08")))
    val b = Seq(ScanResultEntry.forAggregate("v2", Map("bizDateTime" -> "2010-07-08")))

    val actions = DigestDifferencingUtils.differenceAggregates(a, b, Seq(DailyCategoryFunction("bizDateTime")), Seq(unconstrained))
    assertEquals(HashSet(EntityQueryAction(Seq(dateTimeRangeConstraint(JUL_8_2010, endOfDay(JUL_8_2010))))), HashSet(actions: _*))
  }

  @Test
  def shouldRequestIndividualOnMissingMonthVersionsInFirstList {
    val a = Seq()
    val b = Seq(ScanResultEntry.forAggregate("v1", Map("bizDateTime" -> "2010-07")))

    val actions = DigestDifferencingUtils.differenceAggregates(a, b, Seq(MonthlyCategoryFunction("bizDateTime")), Seq(unconstrained))
    assertEquals(HashSet(EntityQueryAction(Seq(dateTimeRangeConstraint(JUL_1_2010, endOfDay(JUL_31_2010))))), HashSet(actions: _*))
  }

  @Test
  def shouldRequestIndividualOnMissingMonthVersionsInSecondList {
    val a = Seq(ScanResultEntry.forAggregate("v1", Map("bizDateTime" -> "2010-07")))
    val b = Seq()

    val actions = DigestDifferencingUtils.differenceAggregates(a, b, Seq(MonthlyCategoryFunction("bizDateTime")), Seq(unconstrained))
    assertEquals(HashSet(EntityQueryAction(Seq(dateTimeRangeConstraint(JUL_1_2010, endOfDay(JUL_31_2010))))), HashSet(actions: _*))
  }

  @Test
  def shouldRequestDayOnMismatchedMonthVersions {
    val a = Seq(ScanResultEntry.forAggregate("v1", Map("bizDateTime" -> "2010-07")))
    val b = Seq(ScanResultEntry.forAggregate("v2", Map("bizDateTime" -> "2010-07")))

    val actions = DigestDifferencingUtils.differenceAggregates(a, b, Seq(MonthlyCategoryFunction("bizDateTime")), Seq(unconstrained))
    assertEquals(HashSet(AggregateQueryAction(Seq(DailyCategoryFunction("bizDateTime")), Seq(dateTimeRangeConstraint(JUL_1_2010, endOfDay(JUL_31_2010))))), HashSet(actions: _*))
  }

  @Test
  def shouldRequestIndividualOnMissingYearVersionsInFirstList {
    val a = Seq()
    val b = Seq(ScanResultEntry.forAggregate("v1", Map("bizDateTime" -> "2010")))

    val actions = DigestDifferencingUtils.differenceAggregates(a, b, Seq(YearlyCategoryFunction("bizDateTime")), Seq(unconstrained))
    assertEquals(HashSet(EntityQueryAction(Seq(dateTimeRangeConstraint(JAN_1_2010, endOfDay(DEC_31_2010))))), HashSet(actions: _*))
  }

  @Test
  def shouldRequestIndividualOnMissingYearVersionsInSecondList {
    val a = Seq(ScanResultEntry.forAggregate("v1", Map("bizDateTime" -> "2010")))
    val b = Seq()

    val actions = DigestDifferencingUtils.differenceAggregates(a, b, Seq(YearlyCategoryFunction("bizDateTime")), Seq(unconstrained))
    assertEquals(HashSet(EntityQueryAction(Seq(dateTimeRangeConstraint(JAN_1_2010, endOfDay(DEC_31_2010))))), HashSet(actions: _*))
  }

  @Test
  def shouldRequestMonthOnMismatchedYearVersions {
    val a = Seq(ScanResultEntry.forAggregate("v1", Map("bizDateTime" -> "2010")))
    val b = Seq(ScanResultEntry.forAggregate("v2", Map("bizDateTime" -> "2010")))

    val actions = DigestDifferencingUtils.differenceAggregates(a, b, Seq(YearlyCategoryFunction("bizDateTime")), Seq(unconstrained))
    assertEquals(HashSet(AggregateQueryAction(Seq(MonthlyCategoryFunction("bizDateTime")), Seq(dateTimeRangeConstraint(JAN_1_2010, endOfDay(DEC_31_2010))))), HashSet(actions: _*))
  }
}