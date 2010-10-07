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

package net.lshift.diffa.participants.web

import net.lshift.diffa.participants.domain.ParticipantEntity
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.{ModelAttribute, RequestParam, RequestMethod, RequestMapping}
import net.lshift.diffa.kernel.participants.UpstreamMemoryParticipant
import org.joda.time.DateTime
import net.lshift.diffa.participants.{DownstreamWebParticipant, UpstreamWebParticipant}
import org.joda.time.format.ISODateTimeFormat

/**
 * Controller for participant functionality.
 */
@Controller
@RequestMapping(Array("/"))
class ParticipantUIController(upstream:UpstreamWebParticipant, downstream:DownstreamWebParticipant) {
  private val dateParser = ISODateTimeFormat.dateTimeParser

  @RequestMapping(method = Array(RequestMethod.GET))
  def home = "home/index";

  @RequestMapping(value = Array("/entities"), method = Array(RequestMethod.GET))
  @ModelAttribute("result")
  def retrieveEntities(@RequestParam("partId") partId: String):Array[ParticipantEntity] = {
    val entitySeq = partId match {
      case "upstream" => upstream.allEntities.map({case (id, entity) => new ParticipantEntity(id, entity.body)})
      case "downstream" => downstream.allEntities.map({case (id, entity) => new ParticipantEntity(id, entity.body)})
    }
    entitySeq.toArray
  }

  @RequestMapping(value = Array("/entities"), method = Array(RequestMethod.POST))
  def updateEntities(@RequestParam("partId") partId: String, @RequestParam("entityId") entityId: String,
                     @RequestParam(value = "lastUpdated", required = false) lastUpdated: String,
                     @RequestParam("body") body: String) = {
    val lastUpdatedDate = lastUpdated match {
      case null => new DateTime
      case ""   => new DateTime
      case s    => dateParser.parseDateTime(s)
    }

    partId match {
      case "upstream"   => upstream.addEntity(entityId, new DateTime, lastUpdatedDate, body)
      case "downstream" => downstream.addEntity(entityId, new DateTime, lastUpdatedDate, body)
    }

    "json/empty"
  }

  @RequestMapping(value = Array("/entities"), method = Array(RequestMethod.DELETE))
  def removeEntity(@RequestParam("partId") partId: String, @RequestParam("entityId") entityId: String) = {
    partId match {
      case "upstream"   => upstream.removeEntity(entityId)
      case "downstream" => downstream.removeEntity(entityId)
    }

    "json/empty"
  }
}