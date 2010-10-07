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

import collection.mutable.HashMap
import net.lshift.diffa.kernel.events.VersionID
import net.lshift.diffa.kernel.differencing.DigestBuilder
import org.joda.time.{Interval, DateTime}

/**
 * Base class for test participants.
 */
class MemoryParticipantBase(nativeVsnGen: String => String) {
  protected val entities = new HashMap[String, TestEntity]

  def queryDigests(start: DateTime, end: DateTime, granularity: RangeGranularity) = {
    // Filter on date interval & sort entries by ID into a list
    val interval = new Interval(start, end)
    val entitiesInRange = entities.values.filter(ent => interval.contains(ent.date)).toList
    val sortedEntities = entitiesInRange.sort(_.id < _.id)

    // Create buckets
    val b = new DigestBuilder(granularity)
    sortedEntities foreach (ent => b.add(ent.id, ent.date, ent.lastUpdated, nativeVsnGen(ent.body)))

    b.digests
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

  def addEntity(id: String, date: DateTime, lastUpdated:DateTime, body: String): Unit = {
    entities += ((id, TestEntity(id, date, lastUpdated, body)))
  }

  def removeEntity(id:String) {
    entities.remove(id)
  }

  def clearEntities {
    entities.clear
  }
}

case class TestEntity(id: String, date: DateTime, lastUpdated:DateTime, body: String)