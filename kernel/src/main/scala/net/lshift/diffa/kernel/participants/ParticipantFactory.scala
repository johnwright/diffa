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

package net.lshift.diffa.kernel.participants

import scala.collection.JavaConversions._ // for implicit conversions Java collections <--> Scala collections

/**
 * Factory that will resolve participant addresses to participant instances for querying.
 */
class ParticipantFactory(val protocolFactories:java.util.List[ParticipantProtocolFactory]) {
    // TODO: Pass this in from config
  private val protobuffers = "application/x-protocol-buffers"
  private val json = "application/json"

  def createUpstreamParticipant(address:String): UpstreamParticipant = {
    findFactory(address).createUpstreamParticipant(address, json)
  }

  def createDownstreamParticipant(address:String): DownstreamParticipant = {
    findFactory(address).createDownstreamParticipant(address, json)
  }

  def findFactory(address:String) =
    protocolFactories.find(f => f.supportsAddress(address, json)) match {
      case None => throw new InvalidParticipantAddressException(address)
      case Some(f) => f
    }
}

class InvalidParticipantAddressException(addr:String)
    extends Exception("The address " + addr + " is not a valid participant address")