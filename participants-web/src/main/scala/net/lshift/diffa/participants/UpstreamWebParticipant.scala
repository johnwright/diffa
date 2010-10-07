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

package net.lshift.diffa.participants

import net.lshift.diffa.kernel.participants.UpstreamMemoryParticipant
import org.joda.time.DateTime
import java.lang.String
import net.lshift.diffa.kernel.events.{VersionID, UpstreamChangeEvent}
import org.apache.commons.codec.digest.DigestUtils

/**
 * An implementation of the UpstreamParticipant using the MemoryParticipant base, whereby the body is the version
 * of an entity.
 */
class UpstreamWebParticipant(epName:String, val agentRoot:String)
    extends UpstreamMemoryParticipant(DigestUtils.md5Hex)
    with WebParticipant {

  override def addEntity(id: String, date: DateTime, lastUpdated:DateTime, body: String) = {
    super.addEntity(id, date, lastUpdated, body)

    changesClient.onChangeEvent(UpstreamChangeEvent(epName, id, date, lastUpdated, body))
  }


  override def removeEntity(id: String) = {
    super.removeEntity(id)

    changesClient.onChangeEvent(UpstreamChangeEvent(epName, id, new DateTime, new DateTime, null))
  }
}