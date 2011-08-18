package net.lshift.diffa.kernel.differencing

import net.lshift.diffa.kernel.events.VersionID
import collection.mutable.HashMap
import net.lshift.diffa.kernel.config.DiffaPairRef
import org.joda.time.{DateTime, Minutes}

class ZoomCache {

  import ZoomCache._

  val levels = DAILY.until(QUARTER_HOURLY)

  val dirtyCaches = new HashMap[Int, HashMap[Int,Int]]

  //levels.foreach(levelCaches(_) = new HashMap[Int,Int])

  def onStore(id: VersionID, seen: DateTime) = {
    val observationDate = nearestObservationDate(new DateTime())

//      levels.foreach(level => {
//        val index = indexOf(observationDate, lastUpdated, level)
//        levelCaches(level).remove()
//      })

  }

  def onMatch(id: VersionID, vsn: String, origin: MatchOrigin) = {
    null
  }

  def getTiles(level:Int) = {
//    dirtyCaches.get(level) match {
//
//    }

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