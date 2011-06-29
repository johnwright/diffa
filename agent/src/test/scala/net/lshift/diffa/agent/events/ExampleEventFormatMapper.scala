package net.lshift.diffa.agent.events

/**
 * Copyright (C) 2010-2011 LShift Ltd.
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

import net.lshift.diffa.kernel.participants.EventFormatMapper
import org.codehaus.jackson.map.ObjectMapper
import net.lshift.diffa.kernel.frontend.wire.WireEvent
import scala.collection.JavaConversions._
import org.slf4j.LoggerFactory
import org.joda.time.format.ISODateTimeFormat.{date, dateTime}
import net.lshift.diffa.kernel.events.UpstreamChangeEventType

/**
 * Content mapper for an example data type.
 */
class ExampleEventFormatMapper extends EventFormatMapper {

  val contentType = "application/example+json"

  private val objectMapper = new ObjectMapper

  private val log = LoggerFactory.getLogger(getClass)

  def map(content: String, endpoint: String) = {
    log.debug("Received change event %s for endpoint %s".format(content, endpoint))

    val in = objectMapper.readTree(content)

    val effectiveDate = date.parseDateTime(in.path("effective-date").getValueAsText)
    val value = in.path("value").getValueAsText
    val id = in.path("id").getValueAsText
    val lastUpdate = date.parseDateTime(in.path("record-date").getValueAsText)
    val vsn = in.path("version").getValueAsText

    Seq(WireEvent(eventType = UpstreamChangeEventType.name,
                  metadata = Map(WireEvent.ID -> id,
                                  WireEvent.ENDPOINT -> endpoint,
                                  WireEvent.LAST_UPDATE -> dateTime.print(lastUpdate),
                                  WireEvent.VSN -> vsn),
                  attributes = List(dateTime.print(effectiveDate), value)))
  }
}