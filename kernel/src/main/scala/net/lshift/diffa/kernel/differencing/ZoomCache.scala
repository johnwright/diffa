package net.lshift.diffa.kernel.differencing

import net.lshift.diffa.kernel.events.VersionID
import collection.mutable.{HashMap,HashSet}
import net.lshift.diffa.kernel.config.DiffaPairRef
import scala.collection.JavaConversions._
import net.sf.ehcache.{Element, CacheManager}
import java.io.Closeable
import org.joda.time.{DateTime, Interval, Minutes}

trait ZoomCache extends Closeable {
  def onStoreUpdate(event: DifferenceEvent, previousDetectionTime:DateTime)
  def retrieveTilesForZoomLevel(level:Int, timestamp:DateTime) : TileSet
}

// TODO Think of making this cache global accross all pairs so that LRU
// can be used accross the entire system
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

  levels.foreach( dirtyTilesByLevel.put(_, new HashSet[Int]) )

  def close() = {
    dirtyTilesByLevel.close()
    tileCachesByLevel.close()
  }

  /**
   * Marks the tile (on each cached level) that corresponds to this version as dirty
   */
  def onStoreUpdate(event: DifferenceEvent, previousDetectionTime:DateTime) = {
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

object ZoomCache {

  val QUARTER_HOURLY = 6
  val HALF_HOURLY = 5
  val HOURLY = 4
  val TWO_HOURLY = 3
  val FOUR_HOURLY = 2
  val EIGHT_HOURLY = 1
  val DAILY = 0

  val levels = DAILY.to(QUARTER_HOURLY)

  val zoom = Map(
    DAILY -> 60 * 24,
    EIGHT_HOURLY -> 60 * 8,
    FOUR_HOURLY -> 60 * 4,
    TWO_HOURLY -> 60 * 2,
    HOURLY -> 60,
    HALF_HOURLY -> 30,
    QUARTER_HOURLY -> 15
  )

  def nearestObservationDate(timestamp:DateTime) = {
    val bottomOfHour = timestamp.withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)
    val increments = timestamp.getMinuteOfHour / 15
    bottomOfHour.plusMinutes( (increments + 1) * 15 )
  }

  def intervalFromIndex(index:Int, level:Int, timestamp:DateTime) = {
    val minutes = zoom(level) * index
    val observationDate = nearestObservationDate(timestamp)
    val rangeEnd = observationDate.minusMinutes(minutes)
    new Interval(rangeEnd.minusMinutes(zoom(level)), rangeEnd)
  }

  def indexOf(observation:DateTime, event:DateTime, level:Int) : Int = {
    validateLevel(level)
    validateTime(observation, event)
    val minutes = Minutes.minutesBetween(event,observation).getMinutes
    minutes / zoom(level)
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