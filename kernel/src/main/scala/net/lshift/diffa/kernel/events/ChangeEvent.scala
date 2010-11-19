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

package net.lshift.diffa.kernel.events

import org.joda.time.DateTime

/**
 * Base inherited by the various types of events.
 * Note that this is the external representation of a change, as opposed to the internal PairChangeEvent type
 */
abstract class ChangeEvent {
  def endpoint:String
  def id:String
  def attributes:Seq[String]
  def lastUpdate:DateTime
}

/**
 * Event indicating that a change has occurred within an upstream system.
 */
case class UpstreamChangeEvent(endpoint:String, id:String, attributes:Seq[String], lastUpdate:DateTime, vsn:String)
  extends ChangeEvent

/**
 * Event indicating that a change has occurred within a downsteam system.
 */
case class DownstreamChangeEvent(endpoint:String, id:String, attributes:Seq[String], lastUpdate:DateTime, vsn:String)
  extends ChangeEvent

/**
 * Event indicating that a correlatable change has occurred within a downstream system. A correlatable downstream
 * change indicates that the change occurring in the downstream did not result in the same content being at the
 * downstream, but provides details on correlating the version information between the systems.
 */
case class DownstreamCorrelatedChangeEvent(endpoint:String, id:String, attributes:Seq[String], lastUpdate:DateTime,  upstreamVsn:String, downstreamVsn:String)
  extends ChangeEvent