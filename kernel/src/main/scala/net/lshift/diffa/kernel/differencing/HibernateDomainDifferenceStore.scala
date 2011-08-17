package net.lshift.diffa.kernel.differencing

import org.joda.time.{DateTime, Interval}
import net.lshift.diffa.kernel.events.VersionID
import net.lshift.diffa.kernel.config.DiffaPairRef
import reflect.BeanProperty
import net.lshift.diffa.kernel.util.HibernateQueryUtils
import org.hibernate.SessionFactory
import net.lshift.diffa.kernel.util.SessionHelper._
import org.hibernate.Session

/**
 * Hibernate backed Domain Cache provider.
 */
class HibernateDomainDifferenceStore(val sessionFactory:SessionFactory)
    extends DomainDifferenceStore
    with HibernateQueryUtils {

  def removeDomain(domain:String) {
    sessionFactory.withSession(s => {
      executeUpdate(s, "removeDomainDiffs", Map("domain" -> domain))
      executeUpdate(s, "removeDomainPendingDiffs", Map("domain" -> domain))
    })
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
            case MatchState.UNMATCHED =>
              // A difference has gone away. Remove the difference, and add in a match
              s.delete(existing)
              saveAndConvertEvent(s, ReportedDifferenceEvent(null, id, new DateTime, true, vsn, vsn, new DateTime))
          }
      }
    })
  }

  def matchEventsOlderThan(pair:DiffaPairRef, cutoff: DateTime) = sessionFactory.withSession(s => {
    listQuery[ReportedDifferenceEvent](s, "unmatchedEventsOlderThanCutoffByDomainAndPair",
      Map("domain" -> pair.domain, "pair" -> pair.key, "cutoff" -> cutoff)).map(old => {
        s.delete(old)
        saveAndConvertEvent(s, ReportedDifferenceEvent(null, old.objId, new DateTime, true, old.upstreamVsn, old.upstreamVsn, new DateTime))
      })
  })

  def retrieveUnmatchedEvents(domain:String, interval: Interval) = sessionFactory.withSession(s => {
    listQuery[ReportedDifferenceEvent](s, "unmatchedEventsInIntervalByDomain",
      Map("domain" -> domain, "start" -> interval.getStart, "end" -> interval.getEnd)).map(_.asDifferenceEvent)
  })

  def retrievePagedEvents(pair: DiffaPairRef, interval: Interval, offset: Int, length: Int) = sessionFactory.withSession(s => {
    listQuery[ReportedDifferenceEvent](s, "unmatchedEventsInIntervalByDomainAndPair",
        Map("domain" -> pair.domain, "pair" -> pair.key, "start" -> interval.getStart, "end" -> interval.getEnd),
        Some(offset), Some(length)).
      map(_.asDifferenceEvent)
  })

  def countEvents(pair: DiffaPairRef, interval: Interval):Int =  sessionFactory.withSession(s => {
    val count:Option[java.lang.Long] = singleQueryOpt[java.lang.Long](s, "countEventsInIntervalByDomainAndPair",
      Map("domain" -> pair.domain, "pair" -> pair.key, "start" -> interval.getStart, "end" -> interval.getEnd))

    count.getOrElse(new java.lang.Long(0L)).intValue
  })

  def retrieveEventsSince(domain: String, evtSeqId: String) = sessionFactory.withSession(s => {
    listQuery[ReportedDifferenceEvent](s, "eventsSinceByDomain",
      Map("domain" -> domain, "seqId" -> Integer.parseInt(evtSeqId))).map(_.asDifferenceEvent)
  })

  def retrieveTiledEvents(domain:String, zoomLevel:Int) = {
    Map()
  }

  def getEvent(domain:String, evtSeqId: String) = sessionFactory.withSession(s => {
    singleQueryOpt[ReportedDifferenceEvent](s, "eventByDomainAndSeqId",
        Map("domain" -> domain, "seqId" -> Integer.parseInt(evtSeqId))) match {
      case None       => throw new InvalidSequenceNumberException(evtSeqId)
      case Some(evt)  => evt.asDifferenceEvent
    }
  })

  def clearAllDifferences = sessionFactory.withSession(s => {
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
          case MatchState.UNMATCHED =>
            // We've already got an unmatched event. See if it matches all the criteria.
            if (existing.upstreamVsn == reportableUnmatched.upstreamVsn && existing.downstreamVsn == reportableUnmatched.downstreamVsn) {
              // Update the last time it was seen
              existing.lastSeen = reportableUnmatched.lastSeen
              s.update(existing)

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
  private def saveAndConvertEvent(s:Session, evt:ReportedDifferenceEvent) = {
    val seqId = s.save(evt).asInstanceOf[java.lang.Integer]
    evt.seqId = seqId
    evt.asDifferenceEvent
  }
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

case class ReportedDifferenceEvent(
  @BeanProperty var seqId:java.lang.Integer = null,
  @BeanProperty var objId:VersionID = null,
  @BeanProperty var detectedAt:DateTime = null,
  @BeanProperty var isMatch:Boolean = false,
  @BeanProperty var upstreamVsn:String = null,
  @BeanProperty var downstreamVsn:String = null,
  @BeanProperty var lastSeen:DateTime = null
) {
  
  def this() = this(seqId = null)

  def asDifferenceEvent = DifferenceEvent(seqId.toString, objId, detectedAt, state, upstreamVsn, downstreamVsn, lastSeen)
  def state = if (isMatch) MatchState.MATCHED else MatchState.UNMATCHED
}