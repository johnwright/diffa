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
import java.lang.IllegalStateException

/**
 * Factory that will resolve participant addresses to participant instances for querying.
 */
class ParticipantFactory() {

  private val protocolFactories = new ListBuffer[ParticipantProtocolFactory]
  private val scanningFactories = new ListBuffer[ScanningParticipantFactory]

  def registerFactory(f:ParticipantProtocolFactory) = protocolFactories += f
  def registerScanningFactory(f:ScanningParticipantFactory) = scanningFactories += f

  def createUpstreamParticipant(endpoint:Endpoint): UpstreamParticipant = {
    val scanningParticipant = createScanningParticipant(endpoint)

    val (address, contentType) = (endpoint.url, endpoint.contentType)
    val uPart = findFactory(protocolFactories, address, contentType).createUpstreamParticipant(address, contentType)

    new CompositeUpstreamParticipant(uPart, scanningParticipant)
  }

  def createDownstreamParticipant(endpoint:Endpoint): DownstreamParticipant = {
    val scanningParticipant = createScanningParticipant(endpoint)

    val (address, contentType) = (endpoint.url, endpoint.contentType)
    val dPart = findFactory(protocolFactories, address, contentType).createDownstreamParticipant(address, contentType)

    new CompositeDownstreamParticipant(dPart, scanningParticipant)
  }

  def createScanningParticipant(endpoint:Endpoint): Option[ScanningParticipantRef] = endpoint.scanUrl match {
    case null => None
    case scanUrl =>
      scanningFactories.
          find(f => f.supportsAddress(scanUrl, endpoint.contentType)).
          map(_.createScanningParticipantRef(scanUrl, endpoint.contentType))
  }

  private def findFactory[T <: AddressDrivenFactory](factories:Seq[T], address:String, contentType:String):T =
    factories.find(f => f.supportsAddress(address, contentType)) match {
      case None => throw new InvalidParticipantAddressException(address, contentType)
      case Some(f) => f
    }

  private class CompositeParticipant(underlying:Participant, scanning:Option[ScanningParticipantRef]) extends Participant {
    def retrieveContent(identifier: String) =
      underlying.retrieveContent(identifier)

    def queryEntityVersions(constraints: Seq[QueryConstraint]) =
      underlying.queryEntityVersions(constraints)

    def queryAggregateDigests(bucketing: Map[String, CategoryFunction], constraints: Seq[QueryConstraint]) =
      underlying.queryAggregateDigests(bucketing, constraints)

    def scan(constraints: Seq[QueryConstraint], aggregations: Map[String, CategoryFunction]) = scanning match {
      case None        => throw new IllegalStateException("This participant doesn't support scanning")
      case Some(spart) => spart.scan(constraints, aggregations)
    }

    def close() {
      underlying.close()
      scanning.foreach(_.close())
    }
  }

  private class CompositeUpstreamParticipant(uPart:UpstreamParticipant, scanning:Option[ScanningParticipantRef])
      extends CompositeParticipant(uPart, scanning)
      with UpstreamParticipant {
  }

  private class CompositeDownstreamParticipant (dPart:DownstreamParticipant, scanning:Option[ScanningParticipantRef])
      extends CompositeParticipant(dPart, scanning)
      with DownstreamParticipant {
    
    def generateVersion(entityBody: String) = dPart.generateVersion(entityBody)
  }
}

class InvalidParticipantAddressException(addr:String, contentType:String)
    extends Exception("The address " + addr + " is not a valid participant address for the content type " + contentType)