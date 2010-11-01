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

package net.lshift.diffa.kernel.differencing

import reflect.BeanProperty
import org.joda.time.DateTime
import scala.collection.Map

// Base type for upstream and downstream correlations allowing pairs to be managed
case class Correlation(
  @BeanProperty var oid:java.lang.Integer,
  @BeanProperty var pairing:String,
  @BeanProperty var id:String,
  // TODO [#2] the attributes go into the index, not the DB
  @BeanProperty var attributes:Map[String,String],
  @BeanProperty var lastUpdate:DateTime,
  @BeanProperty var timestamp:DateTime,
  @BeanProperty var upstreamVsn:String,
  @BeanProperty var downstreamUVsn:String,
  @BeanProperty var downstreamDVsn:String,
  @BeanProperty var isMatched:java.lang.Boolean
) {
  def this() = this(null, null, null, null, null, null, null, null, null, null)
}

object Correlation {
  def asDeleted(pairing:String, id:String, lastUpdate:DateTime) =
    Correlation(null, pairing, id, null, lastUpdate, new DateTime, null, null, null, true)
}