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

package net.lshift.diffa.kernel.util

import net.lshift.diffa.kernel.config.{Pair => DiffaPair}
import net.lshift.diffa.kernel.differencing.{MatchOrigin, VersionPolicy, DifferencesManager}

/**
 * Provides some generic routines to maintain the correlation and diff stores.
 * The aim is to reduce code duplication and make units of work more testable.
 */
object StoreSynchronizationUtils {


  /**
   * Runs a simple replayUnmatchedDifferences for the pair.
   */
  def replayCorrelationStore(diffsManager:DifferencesManager, policy:VersionPolicy, pair:DiffaPair, origin:MatchOrigin) = {

    val diffWriter = diffsManager.createDifferenceWriter(pair.domain.name, pair.key, overwrite = true)
    try {
      val version = diffsManager.lastRecordedVersion(pair.asRef)
      policy.replayUnmatchedDifferences(pair, diffWriter, origin, version)
      diffWriter.close()
    } catch {
      case ex =>
        diffWriter.abort()
        throw ex      // The exception will be logged below. This block is simply to ensure that abort is called.
    }
  }

}