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

package net.lshift.diffa.messaging.json

import org.joda.time.format.ISODateTimeFormat
import org.joda.time.DateTime
import org.codehaus.jackson.map.ser.CustomSerializerFactory
import org.codehaus.jackson.map._
import deser.{StdDeserializerProvider, CustomDeserializerFactory}
import org.codehaus.jackson.{JsonToken, JsonParser, JsonGenerator}
import collection.mutable.ListBuffer
import net.lshift.diffa.kernel.participants._
import org.codehaus.jettison.json.{JSONObject, JSONArray}
import scala.collection.JavaConversions.asList
import org.slf4j.LoggerFactory
import net.lshift.diffa.kernel.frontend.{WireEvent, WireConstraint}

/**
 * Standard utilities for JSON encoding.
 */
object JSONEncodingUtils {

  val log = LoggerFactory.getLogger(getClass)

  val dateEncoder = ISODateTimeFormat.dateTime
  val dateParser = ISODateTimeFormat.dateTimeParser

  val mapper = new ObjectMapper

  def maybeDateStr(date:DateTime) = {
    date match {
      case null => null
      case _    => date.toString(JSONEncodingUtils.dateEncoder)
    }
  }

  def maybeParseableDate(s:String) = {
    s match {
      case null => null
      case ""   => null
      case _    => JSONEncodingUtils.dateParser.parseDateTime(s)
    }
  }

  def serializeEntityContent(content:String) = {
    val node = mapper.createObjectNode
    node.put("content", content)
    node.toString()
  }

  def deserializeEntityContent(wire:String) = mapper.readTree(wire).get("content").getTextValue

  def serialize(constraints:Seq[WireConstraint]) : String = mapper.writeValueAsString(constraints.toArray)
  def deserialize(wire:String) : Seq[WireConstraint]= mapper.readValue(wire, classOf[Array[WireConstraint]])

  def deserializeEntityVersions(wire:String) : Seq[EntityVersion] = {
    deserializeDigests(wire, true).asInstanceOf[Seq[EntityVersion]]
  }

  def deserializeAggregateDigest(wire:String) : Seq[AggregateDigest] = {
    deserializeDigests(wire, false).asInstanceOf[Seq[AggregateDigest]]
  }

  def deserializeEvent(wire:String) : WireEvent = mapper.readValue(wire, classOf[WireEvent])
  def serializeEvent(event:WireEvent) = mapper.writeValueAsString(event)

  private def deserializeDigests(wire:String, readIdField:Boolean) : Seq[Digest] = {

    log.debug("Attempting to deserialize: " + wire)
    
    val buffer = ListBuffer[Digest]()
    val digestArray = new JSONArray(wire)
    for (val i <- 0 to digestArray.length - 1 ) {
      val jsonObject = digestArray.getJSONObject(i)
      val attributeArray = jsonObject.getJSONArray("attributes")
      val attributes = ListBuffer[String]()
      for (val i <- 0 to attributeArray.length - 1 ) {
        attributes += attributeArray.getString(i)
      }
      val lastUpdated = maybeParseableDate(jsonObject.getString("lastUpdated"))
      val digest = jsonObject.getString("digest")
      if (readIdField) {
        val id = jsonObject.getString("id")
        buffer += EntityVersion(id,attributes,lastUpdated,digest)
      }
      else {
        buffer += AggregateDigest(attributes,lastUpdated,digest)
      }
    }
    buffer
  }

  def serializeDigests(digests:Seq[Digest]) : String = {
    log.debug("About to serialize: " + digests)
    val digestArray = new JSONArray
    digests.foreach(d => {
      val digestObject = new JSONObject

      if (d.isInstanceOf[EntityVersion]) {
        digestObject.put("id", d.asInstanceOf[EntityVersion].id)
      }

      digestObject.put("attributes", asList(d.attributes))
      digestObject.put("lastUpdated", d.lastUpdated)
      digestObject.put("digest", d.digest)
      digestArray.put(digestObject)
    })
    val wire = digestArray.toString
    log.debug("Writing to wire: " + wire)
    wire
  }
  
}
