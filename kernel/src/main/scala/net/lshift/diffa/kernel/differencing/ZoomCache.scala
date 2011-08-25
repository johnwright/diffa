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
import net.sf.ehcache.{Element, CacheManager}
import java.io.Closeable
import org.joda.time.{DateTime, Interval, Minutes}

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
   * @param previousDetectionTime Cached time spans that contain this point in time should be invalidated.
   */
  def onStoreUpdate(previousDetectionTime:DateTime)

  /**
   * Retrieves a set of tiles for the current pair.
   * @param level The request level of zoom
   * @param timestamp The point in time from which the zooming should start. In general, this will be the current
   *                  system time, but this parameter is explicitly passed in to make testing easier.
   */
  def retrieveTilesForZoomLevel(level:Int, timestamp:DateTime) : TileSet
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
  private val dirtyTilesByLevel = new CacheWrapper[Int, HashSet[Int]]("dirty", pair,cacheManager)

  /**
   * Cache of indexed tiles for each requested level
   */
  private val tileCachesByLevel = new CacheWrapper[Int, HashMap[Int,Int]]("tiles", pair,cacheManager)

  def close() = {
    dirtyTilesByLevel.close()
    tileCachesByLevel.close()
  }

  /**
   * Marks the tile (on each cached level) that corresponds to this version as dirty
   */
  def onStoreUpdate(previousDetectionTime:DateTime) = {
    val observationDate = nearestObservationDate(new DateTime())
    dirtyTilesByLevel.keys.foreach(level => {
      val index = indexOf(observationDate, previousDetectionTime, level)
      dirtyTilesByLevel.get(level).get += index
    })
  }

  def retrieveTilesForZoomLevel(level:Int, timestamp:DateTime) : TileSet = {

    validateLevel(level)

    val tileCache = tileCachesByLevel.get(level) match {
      case Some(cached) => cached
      case None         =>
        val cache = new HashMap[Int,Int]
        tileCachesByLevel.put(level, cache)

        // Build up an initial cache - after the cache has been primed, it with be invalidated in an event
        // driven fashion

        val observationDate = nearestObservationDate(timestamp)
        var previous = diffStore.previousChronologicalEvent(pair, observationDate)

        // Iterate through the diff store to generate aggregate sums of the events in tile
        // aligned buckets

        while(previous.isDefined) {
          val event = previous.get
          val index = indexOf(observationDate, event.lastSeen, level)
          val interval = intervalFromIndex(0, level, event.lastSeen)
          val events = diffStore.countEvents(pair, interval)
          cache(index) = events
          previous = diffStore.previousChronologicalEvent(pair, interval.getStart)
        }

        // Initialize a dirty flag set for this cache
        dirtyTilesByLevel.put(level, new HashSet[Int])

        cache
    }

    dirtyTilesByLevel.get(level) match {
      case None        => // The tile cache does not need to be preened
      case Some(flags) =>
        flags.map(index => {
          val interval = intervalFromIndex(index, level, timestamp)
          diffStore.countEvents(pair, interval) match {
            case 0 => tileCache -= index  // Remove the cache entry if there are no events
            case n => tileCache(index) = n
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
   *  Given a particular point in time, this returns the nearest tile aligned observation point.
   *  This is used for example to work out what time the right hand side of the heatmap should be aligned to
   *  at any given point in time.
   */
  def nearestObservationDate(timestamp:DateTime) = {
    val bottomOfHour = timestamp.withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)
    val increments = timestamp.getMinuteOfHour / 15
    bottomOfHour.plusMinutes( (increments + 1) * 15 )
  }

  /**
   * Given a particular index offset at particular level, return the time interval of the tile in question.
   * To establish the absolute tile interval, this query needs to have a observation point in time passed
   * into it so that the resulting time span can be calibrated.
   */
  def intervalFromIndex(index:Int, zoomLevel:Int, timestamp:DateTime) = {
    val minutes = zoom(zoomLevel) * index
    val observationDate = nearestObservationDate(timestamp)
    val rangeEnd = observationDate.minusMinutes(minutes)
    new Interval(rangeEnd.minusMinutes(zoom(zoomLevel)), rangeEnd)
  }

  /**
   * From the stand point of a particular observation date, this will establish what tile a particular timestamp
   * would fall into at a given level of zoom
   */
  def indexOf(observation:DateTime, event:DateTime, zoomLevel:Int) : Int = {
    validateLevel(zoomLevel)
    validateTime(observation, event)
    val minutes = Minutes.minutesBetween(event,observation).getMinutes
    minutes / zoom(zoomLevel)
  }

  def validateLevel(level:Int) = if (level < DAILY || level > QUARTER_HOURLY) {
    throw new InvalidZoomLevelException(level)
  }

  def validateTime(observation:DateTime, event:DateTime) = if (observation.isBefore(event)) {
    throw new InvalidObservationDateException(observation, event)
  }
}

class InvalidZoomLevelException(level:Int) extends Exception("Zoom level: " + level)

class InvalidObservationDateException(observation:DateTime, event:DateTime)
  extends Exception("ObservationDate %s is before event date %s ".format(observation, event))

/**
 * Simple wrapper around the underlying EhCache to make its usage less verbose.
 */
class CacheWrapper[A, B](cacheType:String, pair:DiffaPairRef, manager:CacheManager) extends Closeable {

  val cacheName = cacheType + ":" + pair.identifier

  if (manager.cacheExists(cacheName)) {
    manager.removeCache(cacheName)
  }

  manager.addCache(cacheName)

  val cache = manager.getEhcache(cacheName)

  def close() = manager.removeCache(cacheName)

  def get(key:A) : Option[B] = {
    val element = cache.get(key)
    if (element != null) {
      Some(element.getValue.asInstanceOf[B])
    }
    else {
      None
    }
  }

  def put(key:A, value:B) = cache.put(new Element(key,value))

  def keys = cache.getKeys.toList.map(_.asInstanceOf[A])

}