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

package net.lshift.diffa.kernel.events

import org.joda.time.DateTime
import net.lshift.diffa.kernel.frontend.wire.WireEvent

/**
 * Base inherited by the various types of events.
 * Note that this is the external representation of a change, as opposed to the internal PairChangeEvent type
 */
abstract class ChangeEvent {
  def inboundURL:String
  def id:String
  def attributes:Seq[String]
  def lastUpdate:DateTime
  def eventType:ChangeEventType
}

/**
 * Enumeration of possible types of change events.
 */
sealed abstract class ChangeEventType(val name: String) {
  override def toString = name
}
case object UpstreamChangeEventType extends ChangeEventType("upstream")
case object DownstreamChangeEventType extends ChangeEventType("downstream-same")
case object DownstreamCorrelatedChangeEventType extends ChangeEventType("downstream-correlated")

/**
 * Event indicating that a change has occurred within an upstream system.
 */
case class UpstreamChangeEvent(inboundURL:String, id:String, attributes:Seq[String], lastUpdate:DateTime, vsn:String)
  extends ChangeEvent {
  def eventType = UpstreamChangeEventType
}

/**
 * Event indicating that a change has occurred within a downstream system.
 */
case class DownstreamChangeEvent(inboundURL:String, id:String, attributes:Seq[String], lastUpdate:DateTime, vsn:String)
  extends ChangeEvent {
  def eventType = DownstreamChangeEventType
}

/**
 * Event indicating that a correlatable change has occurred within a downstream system. A correlatable downstream
 * change indicates that the change occurring in the downstream did not result in the same content being at the
 * downstream, but provides details on correlating the version information between the systems.
 */
case class DownstreamCorrelatedChangeEvent(inboundURL:String, id:String, attributes:Seq[String], lastUpdate:DateTime,  upstreamVsn:String, downstreamVsn:String)
  extends ChangeEvent {
  def eventType = DownstreamCorrelatedChangeEventType
}