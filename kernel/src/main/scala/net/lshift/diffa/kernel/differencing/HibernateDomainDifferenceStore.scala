package net.lshift.diffa.kernel.differencing

import net.lshift.diffa.kernel.events.VersionID
import net.lshift.diffa.kernel.config.DiffaPairRef
import reflect.BeanProperty
import org.hibernate.SessionFactory
import net.lshift.diffa.kernel.util.SessionHelper._
import org.hibernate.Session
import net.sf.ehcache.CacheManager
import net.lshift.diffa.kernel.util.{Cursor, HibernateQueryUtils}
import java.sql.Timestamp
import scala.collection.JavaConversions._
import org.hibernate.transform.ResultTransformer
import org.joda.time.{DateTimeZone, DateTime, Interval}
import java.math.BigInteger
import java.util.List

/**
 * Hibernate backed Domain Cache provider.
 */
class HibernateDomainDifferenceStore(val sessionFactory:SessionFactory, val cacheManager:CacheManager)
    extends DomainDifferenceStore
    with HibernateQueryUtils {

  val zoomCache = new ZoomCacheProvider(this, cacheManager)

  val zoomQueries = Map(
    ZoomCache.QUARTER_HOURLY -> "15_minute_aggregation"
  )

  def removeDomain(domain:String) {
    sessionFactory.withSession(s => {
      executeUpdate(s, "removeDomainDiffs", Map("domain" -> domain))
      executeUpdate(s, "removeDomainPendingDiffs", Map("domain" -> domain))
    })
  }

  def removePair(pair: DiffaPairRef) = {
    sessionFactory.withSession { s =>
      executeUpdate(s, "removeDiffsByPairAndDomain", Map("pairKey" -> pair.key, "domain" -> pair.domain))
      executeUpdate(s, "removePendingDiffsByPairAndDomain", Map("pairKey" -> pair.key, "domain" -> pair.domain))
    }
  }
  
  def currentSequenceId(domain:String) = sessionFactory.withSession(s => {
    singleQueryOpt[java.lang.Integer](s, "maxSeqIdByDomain", Map("domain" -> domain)).getOrElse(0).toString
  })

  def addPendingUnmatchedEvent(id: VersionID, lastUpdate: DateTime, upstreamVsn: String, downstreamVsn: String, seen: DateTime) {
    sessionFactory.withSession(s => {
      getPendingEvent(s, id) match {
        case None           =>
          val pendingUnmatched = PendingDifferenceEvent(null, id, lastUpdate, upstreamVsn, downstreamVsn, seen)
          val oid = s.save(pendingUnmatched).asInstanceOf[java.lang.Integer]
          pendingUnmatched.oid = oid
        case Some(pending)  =>
          pending.upstreamVsn = upstreamVsn
          pending.downstreamVsn = downstreamVsn
          pending.lastSeen = seen
          s.update(pending)
      }
    })
  }

  def addReportableUnmatchedEvent(id: VersionID, lastUpdate: DateTime, upstreamVsn: String, downstreamVsn: String, seen: DateTime) = sessionFactory.withSession(s => {
    addReportableMismatch(s, ReportedDifferenceEvent(null, id, lastUpdate, false, upstreamVsn, downstreamVsn, seen))
  })

  def upgradePendingUnmatchedEvent(id: VersionID) = sessionFactory.withSession(s => {
    getPendingEvent(s, id) match {
      case None           =>
        // No pending difference, nothing to do
        null
      case Some(pending)  =>
        // Remove the pending and report a mismatch
        s.delete(pending)
        addReportableMismatch(s, pending.convertToUnmatched)
    }
  })

  def cancelPendingUnmatchedEvent(id: VersionID, vsn: String) = sessionFactory.withSession(s => {
    getPendingEvent(s, id).map(pending => {
      if (pending.upstreamVsn == vsn || pending.downstreamVsn == vsn) {
        s.delete(pending)
        true
      } else {
        false
      }
    }).getOrElse(false)
  })

  def addMatchedEvent(id: VersionID, vsn: String) = {
    sessionFactory.withSession(s => {
      // Remove any pending events with the given id
      getPendingEvent(s, id).map(pending => {
        if (pending.upstreamVsn == vsn || pending.downstreamVsn == vsn) {
          s.delete(pending)
        }
      })

      // Find any existing events we've got for this ID
      getEventById(s, id) match {
          // No unmatched event. Nothing to do.
        case None =>
          null
          
        case Some(existing) =>
          existing.state match {
            case MatchState.MATCHED => // Ignore. We've already got an event for what we want.
              existing.asDifferenceEvent
            case MatchState.UNMATCHED | MatchState.IGNORED =>
              // A difference has gone away. Remove the difference, and add in a match
              s.delete(existing)
              val previousDetectionTime = existing.detectedAt
              saveAndConvertEvent(s, ReportedDifferenceEvent(null, id, new DateTime, true, vsn, vsn, new DateTime), previousDetectionTime)
          }
      }
    })
  }

  def matchEventsOlderThan(pair:DiffaPairRef, cutoff: DateTime) = {

     def convertOldEvent(s:Session, old:ReportedDifferenceEvent) {
      s.delete(old)
      val lastSeen = new DateTime
      saveAndConvertEvent(s, ReportedDifferenceEvent(null, old.objId, new DateTime, true, old.upstreamVsn, old.upstreamVsn, lastSeen) )
     }

     val params = Map("domain" -> pair.domain,
                      "pair"   -> pair.key,
                      "cutoff" -> cutoff)

     processAsStream[ReportedDifferenceEvent]("unmatchedEventsOlderThanCutoffByDomainAndPair", params, convertOldEvent)
  }

  def ignoreEvent(domain:String, seqId:String) = {
    sessionFactory.withSession(s => {
      val evt = getOrFail[ReportedDifferenceEvent](s, classOf[ReportedDifferenceEvent], new java.lang.Integer(seqId), "ReportedDifferenceEvent")
      if (evt.objId.pair.domain != domain) {
        throw new IllegalArgumentException("Invalid domain %s for sequence id %s (expected %s)".format(domain, seqId, evt.objId.pair.domain))
      }

      if (evt.isMatch) {
        throw new IllegalArgumentException("Cannot ignore a match for %s (in domain %s)".format(seqId, domain))
      }
      if (!evt.ignored) {
        // Remove this event, and replace it with a new event. We do this to ensure that consumers watching the updates
        // (or even just monitoring sequence ids) see a noticable change.
        s.delete(evt)
        saveAndConvertEvent(s, ReportedDifferenceEvent(null, evt.objId, evt.detectedAt, false,
          evt.upstreamVsn, evt.downstreamVsn, evt.lastSeen, ignored = true))
      } else {
        evt.asDifferenceEvent
      }
    })
  }

  def unignoreEvent(domain:String, seqId:String) = {
    sessionFactory.withSession(s => {
      val evt = getOrFail[ReportedDifferenceEvent](s, classOf[ReportedDifferenceEvent], new java.lang.Integer(seqId), "ReportedDifferenceEvent")
      if (evt.objId.pair.domain != domain) {
        throw new IllegalArgumentException("Invalid domain %s for sequence id %s (expected %s)".format(domain, seqId, evt.objId.pair.domain))
      }
      if (evt.isMatch) {
        throw new IllegalArgumentException("Cannot unignore a match for %s (in domain %s)".format(seqId, domain))
      }
      if (!evt.ignored) {
        throw new IllegalArgumentException("Cannot unignore an event that isn't ignored - %s (in domain %s)".format(seqId, domain))
      }

      // Generate a new event with the same details but the ignored flag cleared. This will ensure consumers
      // that are monitoring for changes will see one.
      s.delete(evt)
      saveAndConvertEvent(s, ReportedDifferenceEvent(null, evt.objId, evt.detectedAt,
        false, evt.upstreamVsn, evt.downstreamVsn, new DateTime))
    })
  }

  def retrieveUnmatchedEvents(domain:String, interval: Interval) = sessionFactory.withSession(s => {
    listQuery[ReportedDifferenceEvent](s, "unmatchedEventsInIntervalByDomain",
      Map("domain" -> domain, "start" -> interval.getStart, "end" -> interval.getEnd)).map(_.asDifferenceEvent)
  })

  def aggregateUnmatchedEvents(pair:DiffaPairRef, interval:Interval, zoomLevel:Int) : Seq[AggregateEvents] = sessionFactory.withSession(s => {

    val query = s.getNamedQuery(zoomQueries(zoomLevel))
    query.setParameter("domain", pair.domain)
    query.setParameter("pair", pair.key)
    query.setParameter("lower_bound", new Timestamp(interval.getStart.minusMonths(1).getMillis))
    query.setParameter("upper_bound", new Timestamp(interval.getEnd.plusMonths(1).getMillis))

    query.setResultTransformer(new ResultTransformer() {
      def transformTuple(tuple: Array[AnyRef], aliases: Array[String]) = {
        //tuple.foreach(t => println("T: " + t + " - " + t.getClass))
        val start = new DateTime(
          tuple(0).asInstanceOf[Int].intValue(),
          tuple(1).asInstanceOf[Int].intValue(),
          tuple(2).asInstanceOf[Int].intValue(),
          tuple(3).asInstanceOf[Int].intValue(),
          tuple(4).asInstanceOf[BigInteger].intValue(),
          0,0,DateTimeZone.UTC)
        val end = start
        val interval = new Interval(start,end)
        AggregateEvents(interval, tuple(5).asInstanceOf[Int].intValue())
      }

      def transformList(collection: List[_]) = collection
    })

    query.list.map(item => item.asInstanceOf[AggregateEvents])
  })

  def retrieveUnmatchedEvents(pair:DiffaPairRef, interval:Interval, f:ReportedDifferenceEvent => Unit) = {

    def processEvent(s:Session, e:ReportedDifferenceEvent) = f(e)

    val params = Map("domain" -> pair.domain,
                      "pair"  -> pair.key,
                      "start" -> interval.getStart,
                      "end"   -> interval.getEnd)

    processAsStream[ReportedDifferenceEvent]("unmatchedEventsInIntervalByDomainAndPair", params, processEvent)
  }

  def retrievePagedEvents(pair: DiffaPairRef, interval: Interval, offset: Int, length: Int, options:EventOptions = EventOptions()) = sessionFactory.withSession(s => {
    val queryName = if (options.includeIgnored) {
      "unmatchedEventsInIntervalByDomainAndPairWithIgnored"
    } else {
      "unmatchedEventsInIntervalByDomainAndPair"
    }

    listQuery[ReportedDifferenceEvent](s, queryName,
        Map("domain" -> pair.domain, "pair" -> pair.key, "start" -> interval.getStart, "end" -> interval.getEnd),
        Some(offset), Some(length)).
      map(_.asDifferenceEvent)
  })

  def countUnmatchedEvents(pair: DiffaPairRef, interval: Interval):Int =  sessionFactory.withSession(s => {
    val count:Option[java.lang.Long] = singleQueryOpt[java.lang.Long](s, "countEventsInIntervalByDomainAndPair",
      Map("domain" -> pair.domain, "pair" -> pair.key, "start" -> interval.getStart, "end" -> interval.getEnd))

    count.getOrElse(new java.lang.Long(0L)).intValue
  })

  def retrieveEventsSince(domain: String, evtSeqId: String) = sessionFactory.withSession(s => {
    listQuery[ReportedDifferenceEvent](s, "eventsSinceByDomain",
      Map("domain" -> domain, "seqId" -> Integer.parseInt(evtSeqId))).map(_.asDifferenceEvent)
  })

  def retrieveEventTiles(pair:DiffaPairRef, zoomLevel:Int, timestamp:DateTime) =
    zoomCache.retrieveTilesForZoomLevel(pair, zoomLevel, timestamp)

  def getEvent(domain:String, evtSeqId: String) = sessionFactory.withSession(s => {
    singleQueryOpt[ReportedDifferenceEvent](s, "eventByDomainAndSeqId",
        Map("domain" -> domain, "seqId" -> Integer.parseInt(evtSeqId))) match {
      case None       => throw new InvalidSequenceNumberException(evtSeqId)
      case Some(evt)  => evt.asDifferenceEvent
    }
  })

  def expireMatches(cutoff:DateTime) {
    sessionFactory.withSession(s => {
      executeUpdate(s, "expireMatches", Map("cutoff" -> cutoff))
    })
  }

  def clearAllDifferences = sessionFactory.withSession(s => {
    zoomCache.clear
    s.createQuery("delete from ReportedDifferenceEvent").executeUpdate()
    s.createQuery("delete from PendingDifferenceEvent").executeUpdate()
  })

  private def getPendingEvent(s:Session, id: VersionID) =
    singleQueryOpt[PendingDifferenceEvent](s, "pendingByDomainIdAndVersionID",
        Map("domain" -> id.pair.domain, "pair" -> id.pair.key, "objId" -> id.id))
  private def getEventById(s:Session, id: VersionID) =
    singleQueryOpt[ReportedDifferenceEvent](s, "eventByDomainAndVersionID",
        Map("domain" -> id.pair.domain, "pair" -> id.pair.key, "objId" -> id.id))
  private def addReportableMismatch(s:Session, reportableUnmatched:ReportedDifferenceEvent) = {
    getEventById(s, reportableUnmatched.objId) match  {
      case Some(existing) =>
        existing.state match {
          case MatchState.IGNORED =>
            if (identicalEventVersions(existing, reportableUnmatched)) {
              // Update the last time it was seen
              existing.lastSeen = reportableUnmatched.lastSeen
              s.update(existing)
              existing.asDifferenceEvent
            } else {
              s.delete(existing)
              reportableUnmatched.ignored = true
              saveAndConvertEvent(s, reportableUnmatched)
            }
          case MatchState.UNMATCHED =>
            // We've already got an unmatched event. See if it matches all the criteria.
            if (identicalEventVersions(existing, reportableUnmatched)) {
              // Update the last time it was seen
              existing.lastSeen = reportableUnmatched.lastSeen
              s.update(existing)
              updateZoomCache(existing.objId.pair, reportableUnmatched.detectedAt)
              existing.asDifferenceEvent
            } else {
              s.delete(existing)
              saveAndConvertEvent(s, reportableUnmatched)
            }

          case MatchState.MATCHED =>
              // The difference has re-occurred. Remove the match, and add a difference.
            s.delete(existing)
            saveAndConvertEvent(s, reportableUnmatched)
        }

      case None =>
        saveAndConvertEvent(s, reportableUnmatched)
    }
  }

  private def identicalEventVersions(first:ReportedDifferenceEvent, second:ReportedDifferenceEvent) =
    first.upstreamVsn == second.upstreamVsn && first.downstreamVsn == second.downstreamVsn


  private def saveAndConvertEvent(s:Session, evt:ReportedDifferenceEvent) = {
    updateZoomCache(evt.objId.pair, evt.detectedAt)
    persistAndConvertEventInternal(s, evt)
  }

  private def saveAndConvertEvent(s:Session, evt:ReportedDifferenceEvent, previousDetectionTime:DateTime) = {
    updateZoomCache(evt.objId.pair, previousDetectionTime)
    persistAndConvertEventInternal(s, evt)
  }

  private def persistAndConvertEventInternal(s:Session, evt:ReportedDifferenceEvent) = {
    val seqId = s.save(evt).asInstanceOf[java.lang.Integer]
    evt.seqId = seqId
    evt.asDifferenceEvent
  }

  private def updateZoomCache(pair:DiffaPairRef, detectedAt:DateTime) = zoomCache.onStoreUpdate(pair, detectedAt)
}

case class PendingDifferenceEvent(
  @BeanProperty var oid:java.lang.Integer = null,
  @BeanProperty var objId:VersionID = null,
  @BeanProperty var detectedAt:DateTime = null,
  @BeanProperty var upstreamVsn:String = null,
  @BeanProperty var downstreamVsn:String = null,
  @BeanProperty var lastSeen:DateTime = null
) {

  def this() = this(oid = null)

  def convertToUnmatched = ReportedDifferenceEvent(null, objId, detectedAt, false, upstreamVsn, downstreamVsn, lastSeen)
}

case class AggregateEventsRow(
  @BeanProperty var year:java.lang.Integer = null,
  @BeanProperty var month:java.lang.Integer = null,
  @BeanProperty var day:java.lang.Integer = null,
  @BeanProperty var hour:java.lang.Integer = null,
  @BeanProperty var minute:java.lang.Integer = null,
  @BeanProperty var aggregate:java.lang.Integer = null
) {
  def this() = this(year = null)
}