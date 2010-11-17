/**
 * Copyright (C) 2010 LShift Ltd.
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

import java.lang.String
import org.joda.time.DateTime
import net.lshift.diffa.kernel.util.HibernateQueryUtils
import net.lshift.diffa.kernel.util.SessionHelper._
import net.lshift.diffa.kernel.indexing.{Indexable, AttributeIndexer}
import net.lshift.diffa.kernel.participants._
// for 'SessionFactory.withSession'
import org.hibernate.criterion.{Restrictions, Order}
import org.hibernate.{Session, SessionFactory}
import net.lshift.diffa.kernel.events.VersionID
import scala.collection.JavaConversions._ // for implicit conversions Java collections <--> Scala collections
import scala.collection.Map

/**
 * Hibernate backed implementation of the Version Correlation store.
 */
class HibernateVersionCorrelationStore(val sessionFactory:SessionFactory, val indexer:AttributeIndexer)
    extends VersionCorrelationStore
    with HibernateQueryUtils {

  def storeUpstreamVersion(id:VersionID, attributes:Map[String,String], lastUpdated: DateTime, vsn: String) = {
    val timestamp = new DateTime()
    sessionFactory.withSession(s => {
      val saveable = queryCurrentCorrelation(s, id) match {

        case None => Correlation(null, id.pairKey, id.id, attributes, null, lastUpdated, timestamp, vsn, null, null, false)
        case Some(c:Correlation) => {
          c.upstreamVsn = vsn
          updateMatchedState(c)
          c.downstreamAttributes = attributes
          c.lastUpdate = lastUpdated
          c.timestamp = timestamp
          c
        }
      }

      s.save(saveable)
      indexer.index(Seq(Indexable(ParticipantType.UPSTREAM, id.id, attributes)))
      saveable
    })
  }
  
  def storeDownstreamVersion(id:VersionID, attributes:Map[String,String], lastUpdated: DateTime, uvsn: String, dvsn: String) = {
    val timestamp = new DateTime()
    sessionFactory.withSession(s => {
      val saveable = queryCurrentCorrelation(s, id) match {
        case None => Correlation(null, id.pairKey, id.id, null, attributes, lastUpdated, timestamp, null, uvsn, dvsn, false)
        case Some(c:Correlation) => {
          c.downstreamUVsn = uvsn
          c.downstreamDVsn = dvsn
          c.downstreamAttributes = attributes
          c.lastUpdate = lastUpdated
          c.timestamp = timestamp
          updateMatchedState(c)
          c
        }
      }

      s.save(saveable)
      indexer.index(Seq(Indexable(ParticipantType.DOWNSTREAM, id.id, attributes)))
      saveable
    })
  }

  def unmatchedVersions(pairKey:String, constraints:Seq[QueryConstraint]) = {
    sessionFactory.withSession(s => {
      val criteria = buildCriteria(s, pairKey, constraints, ParticipantType.UPSTREAM, ParticipantType.DOWNSTREAM)
      criteria.add(Restrictions.eq("isMatched", false))
      criteria.list.map { i => i.asInstanceOf[Correlation] }
    })
  }

  def retrieveCurrentCorrelation(id:VersionID) =
    sessionFactory.withSession(s => queryCurrentCorrelation(s, id))

  def clearUpstreamVersion(id:VersionID) = {
    val timestamp = new DateTime()
    sessionFactory.withSession(s => {
      queryCurrentCorrelation(s, id) match {
        case None => {
          // Generate a new matched correlation detail
          Correlation.asDeleted(id.pairKey, id.id, timestamp)
        }
        case Some(c:Correlation) => {
          c.upstreamVsn = null

          val correlation = if (c.downstreamUVsn == null && c.downstreamDVsn == null) {
                              // No versions at all. We can remove the entity
                              s.delete(c)

                              // Generate a new matched correlation detail
                              Correlation.asDeleted(c.pairing, c.id, timestamp)
                            } else {
                              updateMatchedState(c)
                              s.save(c)
                              c
                            }
          indexer.deleteAttribute(ParticipantType.UPSTREAM,id.id)
          correlation
        }
      }
    })
  }

  def clearDownstreamVersion(id:VersionID) = {
    val timestamp = new DateTime()
    sessionFactory.withSession(s => {
      queryCurrentCorrelation(s, id) match {
        case None => {
          // Generate a new matched correlation detail
          Correlation.asDeleted(id.pairKey, id.id, timestamp)
        }
        case Some(c:Correlation) => {
          c.downstreamUVsn = null
          c.downstreamDVsn = null
          val correlation = if (c.upstreamVsn == null) {
                              // No versions at all. We can remove the entity
                              s.delete(c)

                              // Generate a new matched correlation detail
                              Correlation.asDeleted(c.pairing, c.id, timestamp)
                            } else {
                              updateMatchedState(c)
                              s.save(c)
                              c
                            }
          indexer.deleteAttribute(ParticipantType.DOWNSTREAM,id.id)
          correlation
        }
      }
    })
  }
  def queryUpstreams(pairKey:String, constraints:Seq[QueryConstraint]) = {
    sessionFactory.withSession(s => {
      val criteria = buildCriteria(s, pairKey, constraints, ParticipantType.UPSTREAM)
      criteria.add(Restrictions.isNotNull("upstreamVsn"))
      criteria.list.map(x => x.asInstanceOf[Correlation]).toSeq
    })
  }

  def queryDownstreams(pairKey:String, constraints:Seq[QueryConstraint]) = {
    sessionFactory.withSession(s => {
      val criteria = buildCriteria(s, pairKey, constraints, ParticipantType.DOWNSTREAM)
      criteria.add(Restrictions.or(Restrictions.isNotNull("downstreamUVsn"), Restrictions.isNotNull("downstreamDVsn")))
      criteria.list.map(x => x.asInstanceOf[Correlation]).toSeq
    })
  }

  def queryUpstreams(pairKey:String, constraints:Seq[QueryConstraint], handler:UpstreamVersionHandler) = {
    queryUpstreams(pairKey, constraints).foreach(c => {
      handler(VersionID(c.pairing, c.id), c.upstreamAttributes.values.toSeq, c.lastUpdate, c.upstreamVsn)
    })
  }

  def queryDownstreams(pairKey:String, constraints:Seq[QueryConstraint], handler:DownstreamVersionHandler) = {
    queryDownstreams(pairKey, constraints).foreach(c => {
      handler(VersionID(c.pairing, c.id), c.downstreamAttributes.values.toSeq, c.lastUpdate, c.downstreamUVsn, c.downstreamDVsn)
    })
  }

  private def buildCriteria(s:Session, pairKey:String, constraints:Seq[QueryConstraint], upOrDown:ParticipantType.ParticipantType*) = {
    val criteria = s.createCriteria(classOf[Correlation])
    criteria.add(Restrictions.eq("pairing", pairKey))

    val indexMatches = constraints.foldLeft(List[Indexable]()) ((a:List[Indexable], x:QueryConstraint) => {
      x match {
        case r:NoConstraint                  => a
        case u:UnboundedRangeQueryConstraint => a
        case r:RangeQueryConstraint          => {
          upOrDown.foldLeft(a) ((_a,y) => {_a ::: indexer.rangeQuery(y, x.category, x.values(0), x.values(1)).toList})
        }
        case l:ListQueryConstraint  => throw new RuntimeException("ListQueryConstraint not yet implemented")
      }
    })

    indexMatches.foreach(x => criteria.add(Restrictions.eq("id", x.id)))
    criteria.addOrder(Order.asc("id"))
    criteria
  }

  private def queryCurrentCorrelation(s:Session, id:VersionID):Option[Correlation] =
    singleQueryOpt(s, "currentCorrelation", Map("key" -> id.pairKey, "id" -> id.id))

  private def updateMatchedState(c:Correlation) = {
    c.isMatched = (c.upstreamVsn == c.downstreamUVsn)
    c
  }

}