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

import collection.mutable.{HashMap,HashSet}
import net.lshift.diffa.kernel.config.DiffaPairRef
import scala.collection.JavaConversions._
import net.sf.ehcache.CacheManager
import java.io.Closeable
import net.lshift.diffa.kernel.util.CacheWrapper
import org.joda.time.{Duration, Interval, DateTime}

/**
 * This provides a cache of difference events that have been summarized into a tile shaped structure according
 * to various levels of zooming. This component is intended primarily as an internal component of the
 * HibernateDomainDifferenceStore as opposed to general usage, but has been extracted as a separate component
 * in order to make it more testable.
 *
 * The cache is scoped on a particular pair, so all operations on it are understood to execute within that context.
 */
trait ZoomCache extends Closeable {

  /**
   * Callback to notify the cache that it should synchronize a particular time span with the underlying difference store.
   *
   * @param detectionTime The detection time of the event that is causing the cache to invalidate itself
   */
  def onStoreUpdate(detectionTime:DateTime)

  /**
   * Retrieves a set of tiles for the current pair with a given time frame.
   *
   * @param level The request level of zoom
   * @param timespan The time range to retrieve tiles for
   */
  def retrieveTilesForZoomLevel(level:Int, timespan:Interval) : TileSet
}

/**
 * This provider exploits the cache management functionality provided by the EhCacheManager, including the
 * ability to evict cached entries based on heap usage, access patterns and the number of elements cached across
 * the entire system.
 */
class ZoomCacheProvider(pair:DiffaPairRef,
                        diffStore:DomainDifferenceStore,
                        cacheManager:CacheManager) extends ZoomCache {

  import ZoomCache._

  /**
   * A bit set of flags to mark each tile as dirty on a per-tile basis
   */
  private val dirtyTilesByLevel = new CacheWrapper[Int, HashSet[DateTime]](cacheName("dirty", pair), cacheManager)

  /**
   * Cache of indexed tiles for each requested level
   */
  private val tileCachesByLevel = new CacheWrapper[Int, HashMap[DateTime,Int]](cacheName("tiles", pair), cacheManager)

  private def cacheName(cacheType:String, pair:DiffaPairRef) = cacheType + ":" + pair.identifier

  def close() = {
    dirtyTilesByLevel.close()
    tileCachesByLevel.close()
  }

  /**
   * Marks the tile (on each cached level) that corresponds to this version as dirty
   */
  def onStoreUpdate(detectionTime:DateTime) = {
    dirtyTilesByLevel.keys.foreach(level => {
      val interval = containingInterval(detectionTime, level)
      dirtyTilesByLevel.get(level).get += interval.getStart
    })
  }

  def retrieveTilesForZoomLevel(level:Int, timespan:Interval) : TileSet = {

    validateLevel(level)

    val tileCache = tileCachesByLevel.get(level) match {
      case Some(cached) => cached
      case None         =>
        val cache = new HashMap[DateTime,Int]
        tileCachesByLevel.put(level, cache)

        // Build up an initial cache - after the cache has been primed, it with be invalidated in an event
        // driven fashion

        val alignedStart = containingInterval(timespan.getStart, level).getStart
        val alignedEnd = containingInterval(timespan.getEnd, level).getEnd

        val alignedTimespan = new Interval(alignedStart,alignedEnd)

        // Iterate through the diff store to generate aggregate sums of the events in tile
        // aligned buckets

        diffStore.retrieveUnmatchedEvents(pair, alignedTimespan, (event:ReportedDifferenceEvent) => {
          val intervalStart = containingInterval(event.detectedAt, level).getStart
          cache.get(intervalStart) match {
            case Some(n) => cache(intervalStart) += 1
            case None    => cache(intervalStart)  = 1
          }
        })

        // Initialize a dirty flag set for this cache
        dirtyTilesByLevel.put(level, new HashSet[DateTime])

        cache
    }

    dirtyTilesByLevel.get(level) match {
      case None        => // The tile cache does not need to be preened
      case Some(flags) =>
        flags.map(startTime => {
          val interval = intervalFromStartTime(startTime, level)
          diffStore.countEvents(pair, interval) match {
            case 0 => tileCache -= startTime  // Remove the cache entry if there are no events
            case n => tileCache(startTime) = n
          }
        })
        flags.clear()
     }

    new TileSet(tileCache)
  }

}

/**
 * Provides generically testable definitions and functions for computing tile bounds
 */
object ZoomCache {

  /**
   * A bunch of enums that define what factor of zoom is understood by each level
   */
  val QUARTER_HOURLY = 6
  val HALF_HOURLY = 5
  val HOURLY = 4
  val TWO_HOURLY = 3
  val FOUR_HOURLY = 2
  val EIGHT_HOURLY = 1
  val DAILY = 0

  val levels = DAILY.to(QUARTER_HOURLY)

  /**
   * A lookup table that indicates how many minutes are in a tile at a particular level of zoom.
   */
  val zoom = Map(
    DAILY -> 60 * 24,
    EIGHT_HOURLY -> 60 * 8,
    FOUR_HOURLY -> 60 * 4,
    TWO_HOURLY -> 60 * 2,
    HOURLY -> 60,
    HALF_HOURLY -> 30,
    QUARTER_HOURLY -> 15
  )

  /**
   * Returns a sequence of the start times for the groups of tiles required to build a projection
   * for the given interval at the specified level of zoom.
   */
  def containingTileGroupEdges(interval:Interval, zoomLevel:Int) : Seq[DateTime] = {
    val minutes = interval.toDuration.getStandardMinutes
    val startDay = interval.getStart.getDayOfYear
    val endDay = interval.getEnd.getDayOfYear
    if (minutes >= (24 * 60) || endDay > startDay) { // TODO Corner case between NYE and NYD is not tested
      // Align everything else to days for now, consider a weekly alignment for days or eight hours in due course
      alignToDayBoundary(interval)
    }
    else {
      zoomLevel match {
        case QUARTER_HOURLY | HALF_HOURLY => alignToSubDayBoundary(interval, zoomLevel)
        case _                            => alignToDayBoundary(interval)
      }
    }
  }

  private def alignToSubDayBoundary(interval:Interval, zoomLevel:Int) = {
    val multiple = zoomLevel match {
      case QUARTER_HOURLY => 8
      case HALF_HOURLY    => 12
    }
    val start = subDayAlignedTimestamp(interval.getStart, multiple)
    val end = subDayAlignedTimestamp(interval.getEnd, multiple)
    val times = if (zoomLevel == QUARTER_HOURLY) {
      if ( start.getHourOfDay == 0 && end.getHourOfDay == 16) {
        Set(start, start.withHourOfDay(8), end)
      }
      else {
        Set(start, end)
      }
    } else {
      Set(start, end)
    }
    times.toSeq
  }

  // TODO Copy and paste with subhourly function
  private def subDayAlignedTimestamp(timestamp:DateTime, multiple:Int) = {
    val hourOfDay = timestamp.getHourOfDay
    val alignedHour = hourOfDay - (hourOfDay % multiple)
    timestamp.withHourOfDay(alignedHour).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)
  }

  private def alignToDayBoundary(interval:Interval) = {
    val startTile = interval.getStart.withTimeAtStartOfDay()
    val endTile = interval.getEnd.dayOfYear().roundCeilingCopy()
    val days = new Duration(startTile,endTile).getStandardDays.intValue()
    0.until(days).map(d => startTile.plusDays(d))
  }

  /**
   * Returns a flat list of the tile starting times in the given interval at the specified zoom level
   */
  def individualTileEdges(interval:Interval, zoomLevel:Int) : Seq[DateTime] = {
    val alignedInterval = alignInterval(interval, zoomLevel)
    slice(alignedInterval.toDuration, alignedInterval.getStart, zoomLevel)
  }

  // TODO document
  def alignInterval(interval:Interval, zoomLevel:Int) = {
    val alignedStart = containingInterval(interval.getStart, zoomLevel)
    val alignedEnd = containingInterval(interval.getEnd, zoomLevel)
    new Interval(alignedStart.getStart, alignedEnd.getStart)
  }

  private def slice(d:Duration, startTime:DateTime, zoomLevel:Int) : Seq[DateTime] = {
    val minutes = d.getStandardMinutes.intValue()
    val divisions = minutes / zoom(zoomLevel)
    0.to(divisions).map(d => startTime.plusMinutes(d * zoom(zoomLevel)))
  }

  /**
   * Calculates what the tile aligned interval conatins the given timestamp at the given level of zoom
   */
  def containingInterval(timestamp:DateTime, zoomLevel:Int) = zoomLevel match {
    case DAILY => {
      val start = timestamp.withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)
      new Interval(start, start.plusDays(1))
    }
    case EIGHT_HOURLY => multipleHourlyStart(timestamp, 8)
    case FOUR_HOURLY => multipleHourlyStart(timestamp, 4)
    case TWO_HOURLY => multipleHourlyStart(timestamp, 2)
    case HOURLY => multipleHourlyStart(timestamp, 1)
    case HALF_HOURLY => subHourly(timestamp, 30)
    case QUARTER_HOURLY => subHourly(timestamp, 15)
  }

  private def subHourly(timestamp:DateTime, minutes:Int) = {
    val bottomOfHour = timestamp.withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)
    val increments = timestamp.getMinuteOfHour / minutes
    val start = bottomOfHour.plusMinutes( increments * minutes )
    new Interval(start, start.plusMinutes(minutes))
  }

  private def multipleHourlyStart(timestamp:DateTime, multiple:Int) = {
    val hourOfDay = timestamp.getHourOfDay
    val startHour = hourOfDay - (hourOfDay % multiple)
    val start = timestamp.withHourOfDay(startHour).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)
    new Interval(start, start.plusHours(multiple))
  }

  /**
   * Calculates what the tile aligned interval is having the specified starting timestamp and the given level of zoom
   */
  def intervalFromStartTime(start:DateTime, zoomLevel:Int) = {
    val end = start.plusMinutes(zoom(zoomLevel))
    new Interval(start,end)
  }

  def validateLevel(level:Int) = if (level < DAILY || level > QUARTER_HOURLY) {
    throw new InvalidZoomLevelException(level)
  }

}

class InvalidZoomLevelException(level:Int) extends Exception("Zoom level: " + level)