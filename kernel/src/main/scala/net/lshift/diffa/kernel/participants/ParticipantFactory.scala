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
import net.lshift.diffa.participant.scanning.ScanConstraint
import net.lshift.diffa.kernel.config.{DiffaPairRef, Endpoint}

/**
 * Factory that will resolve participant addresses to participant instances for querying.
 */
class ParticipantFactory() {

  private val scanningFactories = new ListBuffer[ScanningParticipantFactory]
  private val contentFactories = new ListBuffer[ContentParticipantFactory]
  private val versioningFactories = new ListBuffer[VersioningParticipantFactory]

  def registerScanningFactory(f:ScanningParticipantFactory) = scanningFactories += f
  def registerContentFactory(f:ContentParticipantFactory) = contentFactories += f
  def registerVersioningFactory(f:VersioningParticipantFactory) = versioningFactories += f

  def createUpstreamParticipant(endpoint:Endpoint, pair:DiffaPairRef): UpstreamParticipant = {
    val scanningParticipant = createScanningParticipant(endpoint, pair)
    val contentParticipant = createContentParticipant(endpoint, pair)

    new CompositeUpstreamParticipant(endpoint.name, scanningParticipant, contentParticipant)
  }

  def createDownstreamParticipant(endpoint:Endpoint, pair:DiffaPairRef): DownstreamParticipant = {
    val scanningParticipant = createScanningParticipant(endpoint, pair)
    val contentParticipant = createContentParticipant(endpoint, pair)
    val versioningParticipant = createVersioningParticipant(endpoint, pair)

    new CompositeDownstreamParticipant(endpoint.name, scanningParticipant, contentParticipant, versioningParticipant)
  }

  private def nullableToOption[T](v: T): Option[T] = v match {
    case null => None
    case _=> Some(v)
  }
  def createScanningParticipant(endpoint:Endpoint, pair:DiffaPairRef): Option[ScanningParticipantRef] =
    createParticipant(scanningFactories, endpoint, pair, _.scanUrl)
  def createContentParticipant(endpoint:Endpoint, pair:DiffaPairRef): Option[ContentParticipantRef] =
    createParticipant(contentFactories, endpoint, pair, _.contentRetrievalUrl)
  def createVersioningParticipant(endpoint:Endpoint, pair:DiffaPairRef): Option[VersioningParticipantRef] =
    createParticipant(versioningFactories, endpoint, pair, _.versionGenerationUrl)

  private def createParticipant[T](factories:Seq[AddressDrivenFactory[T]], endpoint:Endpoint, pair:DiffaPairRef,
                                    field: Endpoint => Any) (implicit m: Manifest[T]): Option[T] = {
    nullableToOption(field(endpoint)) flatMap { _ =>
      factories.find(f => f.supports(endpoint)) map (
        _.createParticipantRef(endpoint, pair)
        ) orElse {
        throw new InvalidParticipantAddressException(endpoint, m.toString)
      }
    }
  }

  private class CompositeParticipant(partName:String, scanning:Option[ScanningParticipantRef], content:Option[ContentParticipantRef]) extends Participant {
    def retrieveContent(identifier: String) = content match {
      case None         => throw new InvalidParticipantOperationException(partName, "content retrieval")
      case Some(cpart)  => cpart.retrieveContent(identifier)
    }

    def scan(constraints: Seq[ScanConstraint], aggregations: Seq[CategoryFunction]) = scanning match {
      case None        => throw new InvalidParticipantOperationException(partName, "scanning")
      case Some(spart) => spart.scan(constraints, aggregations)
    }

  }

  private class CompositeUpstreamParticipant(partName:String, scanning:Option[ScanningParticipantRef], content:Option[ContentParticipantRef])
      extends CompositeParticipant(partName, scanning, content)
      with UpstreamParticipant {
  }

  private class CompositeDownstreamParticipant(partName:String, scanning:Option[ScanningParticipantRef], content:Option[ContentParticipantRef], versioning:Option[VersioningParticipantRef])
      extends CompositeParticipant(partName, scanning, content)
      with DownstreamParticipant {
    
    def generateVersion(entityBody: String) = versioning match {
      case None        => throw new InvalidParticipantOperationException(partName, "version recovery")
      case Some(vpart) => vpart.generateVersion(entityBody)
    }

  }
}

class InvalidParticipantAddressException(endpoint: Endpoint, kind:String)
    extends Exception("The endpoint " + endpoint + " is not a valid endpoint for "+kind)
class InvalidParticipantOperationException(partName:String, op:String)
    extends Exception("The participant " + partName + " does not support " + op)