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

package net.lshift.diffa.kernel.notifications

import net.lshift.diffa.kernel.events.VersionID
import org.slf4j.{Logger, LoggerFactory}
import net.lshift.diffa.kernel.differencing.{SessionScope, DifferencingListener, SessionManager}
import org.joda.time.{Period, DateTime}
import net.lshift.diffa.kernel.config.ConfigStore
import collection.mutable.ListBuffer
import net.lshift.diffa.kernel.lifecycle.AgentLifecycleAware

class EventNotifier(val sessionManager:SessionManager,
                    val config:ConfigStore,
                    val quietTime:Period)
    extends DifferencingListener
    with AgentLifecycleAware {

  val log:Logger = LoggerFactory.getLogger(getClass)


  log.debug("Quiet time set to " + quietTime.toString)

  private val providers = new ListBuffer[NotificationProvider]()
  private var nextRun = new DateTime()

  def destroy() = {
    // TODO call sessionManager.end() when it is implemented correctly
  }

  override def onAgentConfigurationActivated {
    sessionManager.start(SessionScope.all, this)
  }

  def registerProvider(p:NotificationProvider) = providers += p

  /**
   * Ignore matching events for now because they are not so interesting
   */
  def onMatch(id:VersionID, vsn:String) = null

  def onMismatch(id:VersionID, lastUpdated:DateTime, upstreamVsn:String, downstreamVsn:String) = {
    val now = new DateTime()
    if (now.isAfter(nextRun)) {
      log.info("About to notify users, the received event was " + id + " at " + lastUpdated)
      nextRun = now.plus(quietTime)
      val e = new NotificationEvent(id, lastUpdated, upstreamVsn, downstreamVsn)      
      config.listUsers.foreach(u => providers.foreach( p => p.notify(e,u)))
    }
    
  }

}