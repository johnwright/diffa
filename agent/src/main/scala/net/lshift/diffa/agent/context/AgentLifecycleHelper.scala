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

package net.lshift.diffa.agent.context

import org.springframework.context.event.ContextRefreshedEvent
import org.slf4j.{Logger, LoggerFactory}
import net.lshift.diffa.kernel.lifecycle.AgentLifecycleAware
import org.springframework.context.{ApplicationContext, ApplicationListener}
import scala.collection.JavaConversions._

/**
 * Helper that can trigger specific Agent lifecycle events once the main Spring context has finished it's loading
 * procedure.
 */
class AgentLifecycleHelper extends ApplicationListener[ContextRefreshedEvent] {

  val log:Logger = LoggerFactory.getLogger(getClass)

  def onApplicationEvent(event:ContextRefreshedEvent) {
    fireLifecycle(event.getApplicationContext, _.onAgentAssemblyCompleted)
    fireLifecycle(event.getApplicationContext, _.onAgentConfigurationActivated)

    log.info("Diffa Agent successfully initialized")
  }

  private def fireLifecycle(ctx:ApplicationContext, firer:(AgentLifecycleAware) => Unit) {
    val awareBeans = ctx.getBeansOfType(classOf[AgentLifecycleAware])
    awareBeans.values.foreach(b => firer(b))
  }
}