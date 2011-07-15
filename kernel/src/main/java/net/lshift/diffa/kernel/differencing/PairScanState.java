
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
package net.lshift.diffa.kernel.differencing;

/**
 * Enumeration of the scan states that a pair can be in.
 */
public enum PairScanState {
  /**
   * The state of the pair is unknown, and no scan is currently pending.
   */
  UNKNOWN,

  /**
   * The last scan operation for the pair failed.
   */
  FAILED,

  /**
   * The scan was cancelled on request
   */
  CANCELLED,

  /**
   * The pair is up to date.
   */
  UP_TO_DATE,

  /**
   * The pair is currently being scanned.
   */
  SCANNING
}
