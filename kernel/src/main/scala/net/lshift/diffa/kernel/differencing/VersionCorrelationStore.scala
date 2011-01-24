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

package net.lshift.diffa.kernel.differencing

import org.joda.time.DateTime
import net.lshift.diffa.kernel.events.VersionID
import net.lshift.diffa.kernel.participants.QueryConstraint

/**
 * Store used for caching version correlation information between a pair of participants.
 */
trait VersionCorrelationStore {
  type UpstreamVersionHandler = Function4[VersionID, Map[String, String], DateTime, String, Unit]
  type DownstreamVersionHandler = Function5[VersionID, Map[String, String], DateTime, String, String, Unit]

  /**
   * Stores the details of an upstream version.
   */
  def storeUpstreamVersion(id:VersionID, attributes:Map[String,String], lastUpdated:DateTime, vsn:String):Correlation

  /**
   * Stores the details of a downstream version.
   */
  def storeDownstreamVersion(id:VersionID, attributes:Map[String,String], lastUpdated:DateTime, uvsn:String, dvsn:String):Correlation

  /**
   * Retrieves all of the unmatched version that have been stored.
   */
  def unmatchedVersions(pairKey:String, constraints:Seq[QueryConstraint]) : Seq[Correlation]

  /**
   * Retrieves the current pairing information for the given pairKey/id.
   */
  def retrieveCurrentCorrelation(id:VersionID):Option[Correlation]

  /**
   * Clears the upstream version for the given pairKey/id.
   */
  def clearUpstreamVersion(id:VersionID):Correlation

  /**
   * Clears the downstream version for the given pairKey/id.
   */
  def clearDownstreamVersion(id:VersionID):Correlation

  /**
   * Queries for all upstream versions for the given pair based on the given constraints.
   */
  def queryUpstreams(pairKey:String, constraints:Seq[QueryConstraint], handler:UpstreamVersionHandler)

  /**
   * Queries for all downstream versions for the given pair based on the given constraints.
   */
  def queryDownstreams(pairKey:String, constraints:Seq[QueryConstraint], handler:DownstreamVersionHandler)

  /**
   * Queries for all upstream versions for the given pair based on the given constraints.
   */
  def queryUpstreams(pairKey:String, constraints:Seq[QueryConstraint]) : Seq[Correlation]

  /**
   * Queries for all downstream versions for the given pair based on the given constraints.
   */
  def queryDownstreams(pairKey:String, constraints:Seq[QueryConstraint]) : Seq[Correlation]
}

