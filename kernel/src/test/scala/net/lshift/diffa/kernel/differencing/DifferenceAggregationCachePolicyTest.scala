/**
 * Copyright (C) 2012 LShift Ltd.
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

import org.junit.Assert._

import net.lshift.diffa.kernel.config.DiffaPairRef
import org.joda.time.{DateTimeZone, DateTime}
import org.junit.runner.RunWith
import org.junit.experimental.theories.{Theory, DataPoint, Theories}

@RunWith(classOf[Theories])
class DifferenceAggregationCachePolicyTest {
  import DifferenceAggregationCachePolicyTest._

  @Theory
  def shouldGenerateSequenceKeyThatIncludesDetectionTime(s:KeyForDetectionTimeScenario) {
    val key = DifferenceAggregationCachePolicy.sequenceKeyForDetectionTime(p1, now, s.detected)

    if (key.start != null) assertTrue(s.detected + " should not be before " + key.start, !s.detected.isBefore(key.start))
    if (key.end != null) assertTrue(s.detected + " should be before " + key.end, s.detected.isBefore(key.end))
  }

  @Theory
  def shouldGenerateAppropriateSequenceKeyForDetectionTime(s:KeyForDetectionTimeScenario) {
    assertEquals(
      SequenceCacheKey(p1, s.expectedStart, s.expectedEnd),
      DifferenceAggregationCachePolicy.sequenceKeyForDetectionTime(p1, now, s.detected)
    )
  }
}
object DifferenceAggregationCachePolicyTest {
  // Scenario for determining key for a single detection time
  case class KeyForDetectionTimeScenario(expectedStart:DateTime, expectedEnd:DateTime, detected:DateTime)

  val p1 = DiffaPairRef(domain = "d", key = "p1")

  val now = new DateTime(2012, 4, 23, 8, 30, 0, DateTimeZone.UTC)
  val currentHour = now.withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)

    //
    // Single detection time to key allocation tests
    //

  // Within the first 4 hours should be single hour buckets
  @DataPoint def withinCurrentHourShouldBeSingleHour =
    KeyForDetectionTimeScenario(currentHour, currentHour.plusHours(1), now.minusMinutes(1))
  @DataPoint def twoHoursAgoShouldBeSingleHour =
    KeyForDetectionTimeScenario(currentHour.minusHours(2), currentHour.minusHours(1), now.minusHours(2))
  @DataPoint def threeHoursAgoShouldBeSingleHour =
    KeyForDetectionTimeScenario(currentHour.minusHours(3), currentHour.minusHours(2), now.minusHours(3))

  // Within the range 4-28 hours, should be stored within the same day value
  @DataPoint def fourHoursAgoShouldBeAWholeDay =
    KeyForDetectionTimeScenario(currentHour.minusHours(3).minusDays(1), currentHour.minusHours(3), now.minusHours(4))
  @DataPoint def sixHoursAgoShouldBeAWholeDay =
    KeyForDetectionTimeScenario(currentHour.minusHours(3).minusDays(1), currentHour.minusHours(3), now.minusHours(6))
  @DataPoint def twentySevenHoursAgoShouldBeAWholeDay =
    KeyForDetectionTimeScenario(currentHour.minusHours(3).minusDays(1), currentHour.minusHours(3), now.minusHours(27))

  // Within the range 28-52 hours, should be stored within the same day value
  @DataPoint def twentyEightHoursAgoShouldBeAWholeDay =
    KeyForDetectionTimeScenario(currentHour.minusHours(3).minusDays(2), currentHour.minusHours(3).minusDays(1), now.minusHours(28))
  @DataPoint def fiftyOneHoursAgoShouldBeAWholeDay =
    KeyForDetectionTimeScenario(currentHour.minusHours(3).minusDays(2), currentHour.minusHours(3).minusDays(1), now.minusHours(51))

  // Anything at or before fifty two hours should be stored in a single global cache
  @DataPoint def fiftyTwoHoursAgoShouldBeGlobalBefore =
    KeyForDetectionTimeScenario(null, currentHour.minusHours(3).minusDays(2), now.minusHours(52))
  @DataPoint def oneHundredHoursAgoShouldBeGlobalBefore =
    KeyForDetectionTimeScenario(null, currentHour.minusHours(3).minusDays(2), now.minusHours(100))
}
