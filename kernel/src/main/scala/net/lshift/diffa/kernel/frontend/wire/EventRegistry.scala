package net.lshift.diffa.kernel.frontend.wire

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

import net.lshift.diffa.kernel.events.{DownstreamCorrelatedChangeEvent, DownstreamChangeEvent, UpstreamChangeEvent, ChangeEvent}
import scala.collection.JavaConversions._
import WireEvent._
import org.joda.time.format.ISODateTimeFormat

/**
 * Provides a simple registry to decode a wire event to a kernel event
 */
object EventRegistry {

  val formatter = ISODateTimeFormat.dateTime()

  def resolveEvent(wire:WireEvent) : ChangeEvent = {

    val id = wire.metadata(ID)
    val endpoint = wire.metadata(ENDPOINT)
    val lastUpdate = formatter.parseDateTime(wire.metadata(LAST_UPDATE))

    wire.eventType match {
      case "upstream" => UpstreamChangeEvent(endpoint, id, wire.attributes, lastUpdate, wire.metadata(VSN))
      case "downstream-same" => DownstreamChangeEvent(endpoint, id, wire.attributes, lastUpdate, wire.metadata(VSN))
      case "downstream-correlated" =>
        val up = wire.metadata(UVSN)
        val down = wire.metadata(DVSN)
        DownstreamCorrelatedChangeEvent(endpoint, id, wire.attributes, lastUpdate, up, down)
    }
  }
}