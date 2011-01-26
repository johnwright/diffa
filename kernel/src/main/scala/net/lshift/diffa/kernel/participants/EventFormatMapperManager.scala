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

package net.lshift.diffa.kernel.participants

import scala.collection.mutable.HashMap
import org.slf4j.LoggerFactory

/**
 * Provides a registry for EventFormatMapper instances.
 */
class EventFormatMapperManager {

  private val log = LoggerFactory.getLogger(getClass)

  private val mappers = HashMap[String, EventFormatMapper]()

  def registerMapper(mapper: EventFormatMapper) = {
    log.info("Registered mapper for content type: %s [%s]".format(mapper.contentType, mapper))
    mappers.put(mapper.contentType, mapper)
  }

  def lookup(contentType: String) =
    mappers.get(contentType)
}