/**
 * Copyright (C) 2010-2012 LShift Ltd.
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
package net.lshift.diffa.schema.servicelimits

object ScanConnectTimeout extends ServiceLimit {
  def key = "scan.connect.timeout"
  def description = "When attempting to open a connection to scan a participant, timeout after this many milliseconds"
  def defaultLimit = secondsToMs(30)
  def hardLimit = minutesToMs(2)
}
