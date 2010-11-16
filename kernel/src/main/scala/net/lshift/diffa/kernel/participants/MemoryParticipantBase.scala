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

package net.lshift.diffa.kernel.participants

import scala.collection.Map

import org.joda.time.DateTime
import collection.mutable.HashMap
import net.lshift.diffa.kernel.differencing.DigestBuilder
import org.slf4j.LoggerFactory

/**
 * Base class for test participants.
 */
class MemoryParticipantBase(nativeVsnGen: String => String) {

  val log = LoggerFactory.getLogger(getClass)

  protected val entities = new HashMap[String, TestEntity]

  def queryEntityVersions(constraints:Seq[QueryConstraint]) : Seq[EntityVersion] = {
    log.debug("Running version query: " + constraints)
    val constrained = constrainEntities(constraints)
    constrained.map(e => EntityVersion(e.id,e.attributes, e.lastUpdated,nativeVsnGen(e.body)))
  }

  def queryAggregateDigests(constraints:Seq[QueryConstraint]) : Seq[AggregateDigest] = {
    log.debug("Running aggregate query: " + constraints)
    val constrained = constrainEntities(constraints)
    val b = new DigestBuilder(constraints(0).function)
    constrained foreach (ent => b.add(ent.id, ent.attributes, ent.lastUpdated, nativeVsnGen(ent.body)))
    val digests = b.digests
    log.debug("Returning digests: " + digests)
    digests
//    val start = new DateTime
//    val end = new DateTime
//    val interval = new Interval(start, end)
//    val entitiesInRange = entities.values.filter(ent => interval.contains(ent.date)).toList
//    val sortedEntities = entitiesInRange.sort(_.id < _.id)
//
//    // Create buckets
//    // TODO (#2)  fake granularity
//    val b = new DigestBuilder(IndividualGranularity)
//    sortedEntities foreach (ent => b.add(ent.id, ent.date, ent.lastUpdated, nativeVsnGen(ent.body)))
//
//    b.digests
  }

  def constrainEntities(constraints:Seq[QueryConstraint]) = {
    // Filter on date interval & sort entries by ID into a list
    val entitiesInRange = entities.values.filter(e => true).toList
    entitiesInRange.sort(_.id < _.id)
  }

  def retrieveContent(identifier: String) = entities.get(identifier) match {
    case Some(entity) => entity.body
    case None => null
  }

  def invoke(actionId:String, entityId:String) : ActionResult = {
    actionId match {
      case "resend" => {
        entities.contains(entityId) match {
          case true  => ActionResult("success", entities(entityId).toString)
          case false => ActionResult("error", "Unknown Entity:" + entityId)
        }
      }
      case _        => ActionResult("error", "Unknown action:" + actionId)
    }
  }
  
  def addEntity(id: String, attributes:Seq[String], lastUpdated:DateTime, body: String): Unit = {
    entities += ((id, TestEntity(id, attributes, lastUpdated, body)))
  }

  def removeEntity(id:String) {
    entities.remove(id)
  }

  def clearEntities {
    entities.clear
  }

  def close() = entities.clear
}

case class TestEntity(id: String, attributes:Seq[String], lastUpdated:DateTime, body: String)