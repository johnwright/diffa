/**
 * In Copyright (C) 2010-2011 LShift Ltd.
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

package net.lshift.diffa.kernel.lifecycle

/**
 * Trait implemented by any component that wishes to be informed about Agent-specific lifecycle events.
 */
trait AgentLifecycleAware {
  
  /**
   * Indicates that assembly of the agent has completed. This means that:
   * <ul>
   *   <li>All components have been instantiated and their dependencies have been wired in</li>
   *   <li>Startup methods (as wired in Spring) have been called on all components</li>
   * </ul>
   *
   * This lifecycle event should be listened for by components that need to activate some form of persistent configuration.
   * An example is when a plugin needs to have been booted for it the agent to become operational.
   */
  def onAgentAssemblyCompleted {}

  /**
   * Indicates that persistent configuration within the agent has been restored. This means that:
   * <ul>
   *   <li><code>onAgentAssemblyCompleted</code> preconditions have been satisified</li>
   *   <li>Components that need to re-activate persistent components have done so</li>
   * </ul>
   *
   * This lifecycle event should be listened for by components that need to work with a completely restored agent state.
   */
  def onAgentConfigurationActivated {}
}