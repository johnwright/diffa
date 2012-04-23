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

import net.lshift.diffa.kernel.config.DiffaPairRef
import net.lshift.diffa.kernel.util.CacheWrapper
import net.sf.ehcache.CacheManager
import org.joda.time.{Minutes, Interval, DateTime}
import scala.collection.mutable.{Map => MutableMap}

/**
 * Cache responsible for providing views on aggregated differences.
 */
class DifferenceAggregationCache(diffStore:DomainDifferenceStore, cacheManager:CacheManager) {
  val sequenceCache = new CacheWrapper[SequenceCacheKey, Int]("sequencecache", cacheManager)
  val aggregateCache = new CacheWrapper[AggregateCacheKey, AggregateCacheValue]("aggregatecache", cacheManager)

  def onStoreUpdate(pair:DiffaPairRef, detectionTime:DateTime) = {
    // Remove the stored sequence cache value for the given key
    val k = DifferenceAggregationCachePolicy.sequenceKeyForDetectionTime(pair, now, detectionTime)
    sequenceCache.remove(k)
  }

  def retrieveAggregates(pair:DiffaPairRef, start:DateTime, end:DateTime, aggregateMinutes:Option[Int]) = {
    // Calculate the time buckets that we're after
    val aggregateBounds = aggregateMinutes match {
      case None    => Seq(new Interval(start, end))
      case Some(a) => slice(start, end, a)
    }

    // Work through each aggregate, validate and retrieve it. Retain a session between each call to prevent us having
    // to ask the cache for all the sequence cache keys multiple times
    val session = MutableMap[SequenceCacheKey, Int]()
    aggregateBounds.map(b => retrieveAggregate(pair, b, session))
  }

  def retrieveAggregate(pair:DiffaPairRef, interval:Interval, session:MutableMap[SequenceCacheKey, Int] = MutableMap()) = {
    // Retrieve the sequence cache values that cover this aggregate, then calculate a maximum value
    val seqCacheKeys = DifferenceAggregationCachePolicy.sequenceKeysForDetectionTimeRange(
      pair, now, interval.getStart, interval.getEnd)
    val cacheEntries = seqCacheKeys.
      map(k => session.getOrElseUpdate(k,
        sequenceCache.readThrough(k, () => diffStore.maxSequenceId(pair, k.start, k.end))))
    val maxSeqId = cacheEntries.max

    // Retrieve the value from the aggregate cache, and validate that it matches or exceeds the max sequence id
    val key = AggregateCacheKey(pair, interval.getStart, interval.getEnd)
    val value = aggregateCache.get(key).flatMap(v => {
      if (v.maxSequenceId >= maxSeqId) {
        Some(v)
      } else {
        None
      }
    })

    value match {
      case None    =>
        // Rebuild the value
        val unmatched = diffStore.countUnmatchedEvents(pair, interval.getStart, interval.getEnd)
        aggregateCache.put(key, AggregateCacheValue(unmatched, maxSeqId))
      case Some(v) =>
        v.count
    }
  }

  def now = new DateTime

  private def slice(startTime:DateTime, endTime:DateTime, aggregateMinutes:Int) : Seq[Interval] = {
    val divisions = scala.math.ceil(
      Minutes.minutesBetween(startTime, endTime).getMinutes.asInstanceOf[Double] / aggregateMinutes).toInt

    if (startTime.plusMinutes(divisions * aggregateMinutes) != endTime) {
      throw new IllegalArgumentException("Time range %s minutes (%s -> %s) is not a multiple of %s minutes".format(
        Minutes.minutesBetween(startTime, endTime).getMinutes, startTime, endTime, aggregateMinutes))
    }

    (0 to divisions).
      map(d => new Interval(startTime.plusMinutes(d * aggregateMinutes), startTime.plusMinutes((d + 1) * aggregateMinutes)))
  }
}

/**
 * Key into the sequence cache for a time range on a given pair.
 */
case class SequenceCacheKey(pair:DiffaPairRef, start:DateTime, end:DateTime)

/**
 * Key into the aggregate cache for a time range on a given pair.
 */
case class AggregateCacheKey(pair:DiffaPairRef, start:DateTime, end:DateTime)

/**
 * Value stored in the aggregate cache.
 */
case class AggregateCacheValue(count:Int, maxSequenceId:Int)