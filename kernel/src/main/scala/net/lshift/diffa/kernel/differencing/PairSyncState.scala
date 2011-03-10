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

/**
 * Enumeration of the synchronisation states that a pair can be in.
 */
abstract class PairSyncState

object PairSyncState {
  /**
   * The state of the pair is unknown, and no sync is currently pending.
   */
  case object Unknown extends PairSyncState

  /**
   * The last synchronisation operation for the pair failed.
   */
  case object Failed extends PairSyncState

  /**
   * The pair is up to date.
   */
  case object UpToDate extends PairSyncState

  /**
   * The pair is currently being synchronised.
   */
  case object Synchronising extends PairSyncState
}