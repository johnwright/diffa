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

package net.lshift.diffa.messaging.json

import net.lshift.diffa.kernel.protocol.ProtocolMapper
import net.lshift.diffa.kernel.frontend.Changes
import net.lshift.diffa.kernel.participants.ParticipantFactory

/**
 * Utility class responsible for registering the JSON protocol support with necessary factories
 * and request handlers.
 */
class JsonMessagingRegistrar(val protocolMapper:ProtocolMapper,
                             val participantFactory:ParticipantFactory,
                             val changes:Changes) {

  // Register the outbound participant factory for JSON/HTTP
  val factory = new JSONRestParticipantProtocolFactory()
  participantFactory.registerFactory(factory)

  // Register a handler so requests made to the /changes endpoint on the agent with inbound content types of
  // application/json are decoded by our ChangesHandler.
  protocolMapper.registerHandler("changes", new ChangesHandler(changes))
}