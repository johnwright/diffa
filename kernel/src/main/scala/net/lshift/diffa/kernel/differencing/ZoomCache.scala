package net.lshift.diffa.kernel.differencing

import net.lshift.diffa.kernel.events.VersionID
import collection.mutable.{HashMap,HashSet}
import net.lshift.diffa.kernel.config.DiffaPairRef
import org.joda.time.{DateTime, Interval, Minutes}

// TODO Think of making this cache global accross all pairs so that LRU
// can be used accross the entire system
class ZoomCache(pair:DiffaPairRef, diffStore:DomainDifferenceStore) {

  import ZoomCache._

  /**
   * A bit set of flags to mark each tile as dirty on a per-tile basis
   */
  private val dirtyTilesByLevel = new HashMap[Int, HashSet[Int]]

  /**
   * Cache of indexed tiles for each requested level
   * // TODO We probably want to evict this kind of cache
   */
  private val tileCachesByLevel = new HashMap[Int, HashMap[Int,Int]]

  private val levels = DAILY.until(QUARTER_HOURLY)

  levels.foreach(dirtyTilesByLevel(_) = new HashSet[Int])

  /**
   * Marks the tile (on each cached level) that corresponds to this version as dirty
   */
  def onStore(id: VersionID, lastSeen: DateTime) = {
    val observationDate = nearestObservationDate(new DateTime())
    tileCachesByLevel.keysIterator.foreach(level => {
      val index = indexOf(observationDate, lastSeen, level)
      dirtyTilesByLevel(level) += index
    })
  }

  def retrieveTilesForZoomLevel(level:Int) : Map[Int,Int] = {

    validateLevel(level)

    val tileCache = tileCachesByLevel.get(level) match {
      case Some(cached) => cached
      case None         =>
        val cache = new HashMap[Int,Int]
        tileCachesByLevel(level) = cache

        // Build up an initial cache - after the cache has been primed, it with be invalidated in an event
        // driven fashion

        val observationDate = nearestObservationDate(new DateTime())
        var previous = diffStore.previousChronologicalEvent(pair, observationDate)

        // Iterate through the diff store to generate aggregate sums of the events in tile
        // aligned buckets

        while(previous.isDefined) {
          val event = previous.get
          val index = indexOf(observationDate, event.lastSeen, level)
          val interval = intervalFromIndex(index, level, event.lastSeen)
          val events = diffStore.countEvents(pair, interval)
          cache(index) = events
          previous = diffStore.previousChronologicalEvent(pair, interval.getStart)
        }

        cache
    }

    // Invalidate the cached tiles that are dirty
    dirtyTilesByLevel(level).map(index => {
      val interval = intervalFromIndex(index, level, new DateTime())
      val events = diffStore.countEvents(pair, interval)
      tileCache(level) = events
    })

    // Reset the dirty flags
    dirtyTilesByLevel(level).clear()

    tileCache.toMap
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