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

package net.lshift.diffa.kernel.frontend

import reflect.BeanProperty
import scala.collection.JavaConversions._
import scala.collection.Map
import net.lshift.diffa.kernel.participants.{Digest, EntityVersion, AggregateDigest}
import org.joda.time.format.ISODateTimeFormat

/**
 * This is a structure that is straightforward to pack and unpack onto and off a wire that
 * represents aggregate digests and entity versions.
 */
case class WireDigest(
  @BeanProperty var metadata:java.util.Map[String,String],
  @BeanProperty var attributes:java.util.List[String]) {

  def this() = this(null,null)
  
}

object WireDigest {

  val ID = "id"
  val LAST_UPDATED = "lastUpdated"
  val DIGEST = "digest"

  val dateParser = ISODateTimeFormat.dateTimeParser

  def digest(d:Digest) = {
    val base = collection.immutable.Map(
      LAST_UPDATED -> d.lastUpdated.toString(),
      DIGEST -> d.digest
    )
    val metadata = d match {
      case e:EntityVersion => base(ID) = e.id
      case d:AggregateDigest => base
    }
    WireDigest(metadata, d.attributes)
  }

  def fromWire(wire:WireDigest) : Digest = {
    if (wire.metadata.containsKey(ID)) {
      EntityVersion(wire.metadata(ID),
                    wire.attributes,
                    dateParser.parseDateTime(wire.metadata(LAST_UPDATED)),
                    wire.metadata(DIGEST))
    }
    else {
      AggregateDigest(wire.attributes,
                      dateParser.parseDateTime(wire.metadata(LAST_UPDATED)),
                      wire.metadata(DIGEST))
    }
  }
}