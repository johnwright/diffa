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

package net.lshift.diffa.kernel.alerting

/**
 * Index of alert keys that can be emitted by the kernel.
 */
object KernelAlerts {
  val INTERNAL_ERROR = new AlertKey(ErrorAlert, "FDK000", "An unexpected internal error has occurred within the kernel")

  // Startup/lifecycle events
  val ANALYSER_STARTUP = new AlertKey(InfoAlert, "FDK010", "A realtime analyser has been started")
  val TRANSPORT_LIFECYCLE = new AlertKey(InfoAlert, "FDK011", "A transport component has had a lifecycle change")

  val INVALID_POLICY_EVENT = new AlertKey(ErrorAlert, "FDK020", "An invalid event was received by a policy")
}