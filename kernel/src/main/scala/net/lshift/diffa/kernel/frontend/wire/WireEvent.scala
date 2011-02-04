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
import net.lshift.diffa.kernel.events.{ChangeEvent, DownstreamCorrelatedChangeEvent, DownstreamChangeEvent, UpstreamChangeEvent}
import scala.collection.JavaConversions._

/**
 * This is a structure that is straightforward to pack and unpack onto and off a wire that
 * represents ChangeEvents
 */
case class WireEvent(
  @BeanProperty var eventType:String,
  @BeanProperty var metadata:Map[String,String],
  @BeanProperty var attributes:List[String]) {

  def this() = this(null, null, null)

}

object WireEvent {

  val UPSTREAM = "upstream"
  val DOWNSTREAM = "downstream-same"
  val DOWNSTREAM_CORRELATED = "downstream-correlated"
  val ID = "id"
  val LAST_UPDATE = "lastUpdate"
  val ENDPOINT = "endpoint"
  val VSN = "vsn"
  val DVSN = "dvsn"
  val UVSN = "uvsn"

  def toWire(event:UpstreamChangeEvent) = new WireEvent(event.eventType.toString, extractMetaDataWithVersion(event), event.attributes)
  def toWire(event:DownstreamChangeEvent) = new WireEvent(event.eventType.toString, extractMetaDataWithVersion(event), event.attributes)
  def toWire(event:DownstreamCorrelatedChangeEvent) = {
    val metadata = (extractMetaData(event)(DVSN) = event.downstreamVsn)(UVSN) = event.upstreamVsn
    new WireEvent(event.eventType, metadata, event.attributes)
  }

  def extractMetaData(event:ChangeEvent) = {
    collection.immutable.Map(
      ID -> event.id.toString(),
      ENDPOINT -> event.endpoint.toString(),
      LAST_UPDATE -> event.lastUpdate.toString()
    )
  }

  def extractMetaDataWithVersion(event:UpstreamChangeEvent) = extractMetaData(event)(VSN) = event.vsn
  def extractMetaDataWithVersion(event:DownstreamChangeEvent) = extractMetaData(event)(VSN) = event.vsn

}
