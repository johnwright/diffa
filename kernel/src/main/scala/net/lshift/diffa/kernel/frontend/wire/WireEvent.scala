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

package net.lshift.diffa.kernel.frontend.wire

import reflect.BeanProperty
import java.util.Map
import java.util.List
import scala.collection.JavaConversions._
import org.joda.time.format.ISODateTimeFormat
import net.lshift.diffa.kernel.events._

/**
 * This is a structure that is straightforward to pack and unpack onto and off a wire that
 * represents ChangeEvents
 */
case class WireEvent(
  @BeanProperty var eventType:String,
  @BeanProperty var metadata:Map[String,String],
  @BeanProperty var attributes:List[String]) {

  import WireEvent._

  def this() = this(null, null, null)

  def toKernelEvent: ChangeEvent = {
    val id = metadata(ID)
    val lastUpdate = formatter.parseDateTime(metadata(LAST_UPDATE))

    eventType match {
      case UpstreamChangeEventType.name => UpstreamChangeEvent(id, attributes, lastUpdate, metadata(VSN))
      case DownstreamChangeEventType.name => DownstreamChangeEvent(id, attributes, lastUpdate, metadata(VSN))
      case DownstreamCorrelatedChangeEventType.name =>
        val up = metadata(UVSN)
        val down = metadata(DVSN)
        DownstreamCorrelatedChangeEvent(id, attributes, lastUpdate, up, down)
    }
  }
}

object WireEvent {
  val formatter = ISODateTimeFormat.dateTime()

  val ID = "id"
  val LAST_UPDATE = "lastUpdate"
  val INBOUND_URL = "inboundURL"
  val VSN = "vsn"
  val DVSN = "dvsn"
  val UVSN = "uvsn"

  val UPSTREAM_EVENT = "upstream"
  val DOWNSTREAM_EVENT = "downstream-same"
  val DOWNSTREAM_CORR_EVENT = "downstream-correlated"

  def toWire(event:UpstreamChangeEvent) = new WireEvent(UPSTREAM_EVENT, extractMetaDataWithVersion(event), event.attributes)
  def toWire(event:DownstreamChangeEvent) = new WireEvent(DOWNSTREAM_EVENT, extractMetaDataWithVersion(event), event.attributes)
  def toWire(event:DownstreamCorrelatedChangeEvent) = {
    val metadata = extractMetaData(event) + (DVSN -> event.downstreamVsn) + (UVSN -> event.upstreamVsn)
    new WireEvent(DOWNSTREAM_CORR_EVENT, metadata, event.attributes)
  }

  def extractMetaData(event:ChangeEvent) = {
    collection.immutable.Map(
      ID -> event.id.toString(),
      LAST_UPDATE -> event.lastUpdate.toString()
    )
  }

  def extractMetaDataWithVersion(event:UpstreamChangeEvent) = extractMetaData(event) + (VSN -> event.vsn)
  def extractMetaDataWithVersion(event:DownstreamChangeEvent) = extractMetaData(event) + (VSN -> event.vsn)

}
