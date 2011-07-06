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
package net.lshift.diffa.participant.correlation;

/**
 * Adapter allowing a ContentParticipant to be implemented without requiring it to sub-class the
 * ContentParticipantRequestHandler or the ContentParticipantServlet, and instead be delegated to.
 */
public class VersioningParticipantDelegator extends VersioningParticipantRequestHandler {
  private final VersioningParticipantHandler handler;

  public VersioningParticipantDelegator(VersioningParticipantHandler handler) {
    this.handler = handler;
  }

  @Override
  public ProcessingResponse generateVersion(String entityBody) {
    return handler.generateVersion(entityBody);
  }
}
