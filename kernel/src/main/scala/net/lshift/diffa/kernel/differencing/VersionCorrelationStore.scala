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

import org.joda.time.DateTime
import net.lshift.diffa.kernel.events.VersionID

/**
 * Store used for caching version correlation information between a pair of participants.
 */
trait VersionCorrelationStore {
  type UpstreamVersionHandler = Function4[VersionID, DateTime, DateTime, String, Unit]
  type DownstreamVersionHandler = Function5[VersionID, DateTime, DateTime, String, String, Unit]

  // TODO (#2) generalize the date parameter in {storeUpstreamVersion,storeDownstreamVersion}

  /**
   * Stores the details of an upstream version.
   */
  def storeUpstreamVersion(id:VersionID, date:DateTime, lastUpdated:DateTime, vsn:String):Correlation

  /**
   * Stores the details of a downstream version.
   */
  def storeDownstreamVersion(id:VersionID, date:DateTime, lastUpdated:DateTime, uvsn:String, dvsn:String):Correlation

  /**
   * Retrieves all of the unmatched version that have been stored.
   */
  def unmatchedVersions(pairKey:String, dateRange:DateConstraint):Seq[Correlation]

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
   * Queries for all upstream versions for the given pair based on the given date constraint.
   */
  def queryUpstreams(pairKey:String, dateRange:DateConstraint, handler:UpstreamVersionHandler)

  /**
   * Queries for all downstream versions for the given pair based on the given date constraint.
   */
  def queryDownstreams(pairKey:String, dateRange:DateConstraint, handler:DownstreamVersionHandler)
}

