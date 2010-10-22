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
package net.lshift.diffa.messaging.json

import org.springframework.context.ApplicationListener
import org.springframework.context.event.ContextRefreshedEvent
import org.slf4j.LoggerFactory
import net.lshift.diffa.kernel.protocol.ProtocolMapper
import net.lshift.diffa.kernel.config.ConfigStore
import net.lshift.diffa.kernel.frontend.Changes

/**
 * This registers all of the JSON enabled internal endpoints that are listed
 * in the config store.
 */
class JsonMessagingRegistrar(val protocolMapper:ProtocolMapper,
                             val configStore:ConfigStore) extends ApplicationListener[ContextRefreshedEvent] {

  val log = LoggerFactory.getLogger(getClass)

  val contentType = "application/json"

  def onApplicationEvent(event:ContextRefreshedEvent) {
    val endpoints = configStore.listEndpoints
    endpoints.find( x => (x.contentType == contentType && x.inboundUrl != null) ) match {
      case None    =>
      case Some(e) => {
        val handler = new ChangesHandler(event.getApplicationContext.getBean(classOf[Changes]))
        protocolMapper.registerHandler(e.inboundUrl, handler)
        log.info("Registered endpoint [" + e.inboundUrl + "] for [" + handler.contentType) + "]"
      }
    }
  }
}