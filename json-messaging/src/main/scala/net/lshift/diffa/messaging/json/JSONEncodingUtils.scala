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

/**
 * Standard utilities for JSON encoding.
 */

object JSONEncodingUtils {

  val log = LoggerFactory.getLogger(getClass)

  val dateEncoder = ISODateTimeFormat.dateTime
  val dateParser = ISODateTimeFormat.dateTimeParser

  val serializationFactory = new CustomSerializerFactory()
  serializationFactory.addGenericMapping(classOf[QueryConstraint], new QueryConstraintSerializer())
  val deserializationFactory = new CustomDeserializerFactory()
  deserializationFactory.addSpecificMapping(classOf[QueryConstraint], new QueryConstraintDeserializer())
  val mapper = new ObjectMapper
  mapper.setSerializerFactory(serializationFactory)
  mapper.setDeserializerProvider(new StdDeserializerProvider(deserializationFactory))

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

  def serialize(constraints:Seq[QueryConstraint]) : String = mapper.writeValueAsString(constraints.toArray)
  def deserialize(wire:String) : Seq[QueryConstraint]= mapper.readValue(wire, classOf[Array[QueryConstraint]])

  def deserializeEntityVersions(wire:String) : Seq[EntityVersion] = {
    deserializeDigests(wire, true).asInstanceOf[Seq[EntityVersion]]
  }

  def deserializeAggregateDigest(wire:String) : Seq[AggregateDigest] = {
    deserializeDigests(wire, false).asInstanceOf[Seq[AggregateDigest]]
  }

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

  // TODO I can't believe you have to do this
  def toList[T](a:JSONArray) : Seq[T] = {
    if (null == a) {
      List()
    }
    else {
      for (val i <- 0 to a.length - 1 ) yield a.get(i).asInstanceOf[T]
    }
  }
}

class QueryConstraintSerializer extends JsonSerializer[QueryConstraint] {

  def serialize(constraint: QueryConstraint, jgen: JsonGenerator, provider: SerializerProvider) = {
    jgen.writeStartObject
    jgen.writeFieldName("category")
    jgen.writeString(constraint.category)
    jgen.writeFieldName("function")
    jgen.writeString(constraint.function.getClass.getName)
    jgen.writeArrayFieldStart("values")
    constraint.values.foreach(jgen.writeString(_))    
    jgen.writeEndArray
    jgen.writeEndObject
  }
}

class QueryConstraintDeserializer extends JsonDeserializer[QueryConstraint] {
  def deserialize(parser: JsonParser, ctxt: DeserializationContext) = {

    var category:String = null
    var function:CategoryFunction = null
    val buffer = new ListBuffer[String]

    if(parser.getCurrentToken == JsonToken.START_OBJECT) {
      parser.nextToken
      val label = parser.getText
      asserFieldName(parser, "category")
      parser.nextToken
      category = parser.getText
      parser.nextToken
      asserFieldName(parser, "function")
      parser.nextToken
      val functionName = parser.getText
      function = Class.forName(functionName).newInstance.asInstanceOf[CategoryFunction]
      parser.nextToken
      asserFieldName(parser, "values")
      parser.nextToken
      if(parser.getCurrentToken == JsonToken.START_ARRAY) {
        parser.nextToken
        while (parser.getCurrentToken != JsonToken.END_ARRAY) {
          buffer += parser.getText
          parser.nextToken
        }
      }
    }

    parser.nextToken

    if(parser.getCurrentToken != JsonToken.END_OBJECT) {
      throw new RuntimeException("Wrong token: " + parser.getCurrentToken)      
    }

    RangeQueryConstraint(category, function, buffer)
  }

  def asserFieldName(parser:JsonParser, expectation:String) = {
    if (parser.getText != expectation) {
      throw new RuntimeException("Unexpected field name: " + parser.getText + "; expected: " + expectation)
    }
  }

}