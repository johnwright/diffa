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

package net.lshift.diffa.kernel.protocol

import scala.collection.JavaConversions._ // for implicit conversions Java collections <--> Scala collections
import collection.mutable.{HashSet, HashMap}


/**
 * Manages the mapping of content type information to appropriate protocol handlers.
 */
class ProtocolMapper(groups:java.util.Map[String, java.util.List[ProtocolHandler]]) {
  /**
   * Looks up a handler for the given inbound content type. Returns either Some(handler) or None.
   */
  def lookupHandler(endpointGroup:String, contentType:String):Option[ProtocolHandler] = {
    if (groups.containsKey(endpointGroup)) {
      // Try to find a handler that supports the content type
      val handlers = groups.get(endpointGroup)
      handlers.find(_.contentTypes.contains(contentType))
    } else {
      None
    }
  }

  /**
   * Queries for a list of all valid endpoint names across all registered groups and their handlers.
   */
  def allEndpoints:Map[String, Seq[String]] = {
    val result = new HashMap[String, Seq[String]]
    groups.keySet.foreach(n => {
      val handlers = groups.get(n)
      val handlersRes = new HashSet[String]

      handlers.foreach(h => handlersRes ++= h.endpointNames)
      result(n) = handlersRes.toSeq
    })

    Map.empty ++ result
  }
}