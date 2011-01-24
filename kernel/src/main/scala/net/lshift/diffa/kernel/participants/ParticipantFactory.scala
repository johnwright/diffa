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

package net.lshift.diffa.kernel.participants

import collection.mutable.ListBuffer
import net.lshift.diffa.kernel.config.Endpoint

/**
 * Factory that will resolve participant addresses to participant instances for querying.
 */
class ParticipantFactory() {

  private val protocolFactories = new ListBuffer[ParticipantProtocolFactory]

  def registerFactory(f:ParticipantProtocolFactory) = protocolFactories += f

  def createUpstreamParticipant(endpoint:Endpoint): UpstreamParticipant = {
    val (address, protocol) = (endpoint.url, endpoint.contentType)
    findFactory(address, protocol).createUpstreamParticipant(address, protocol)
  }

  def createDownstreamParticipant(endpoint:Endpoint): DownstreamParticipant = {
    val (address, protocol) = (endpoint.url, endpoint.contentType)
    findFactory(address, protocol).createDownstreamParticipant(address, protocol)
  }

  private def findFactory(address:String, protocol:String) =
    protocolFactories.find(f => f.supportsAddress(address, protocol)) match {
      case None => throw new InvalidParticipantAddressException(address, protocol)
      case Some(f) => f
    }
}

class InvalidParticipantAddressException(addr:String, protocol:String)
    extends Exception("The address " + addr + " is not a valid participant address for the protocol " + protocol)