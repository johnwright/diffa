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

package net.lshift.diffa.kernel.participants

import java.lang.String
/**
 * Downstream participant stub for use in test cases.
 */
class DownstreamMemoryParticipant(val uvsnGen: String => String, val dvsnGen: String => String)
        extends MemoryParticipantBase(dvsnGen)
        with DownstreamParticipant {

  def generateVersion(entityBody: String) = {
    entities.values.toList.find(ent => entityBody.equals(ent.body)) match {
      case Some(entity) => ProcessingResponse(entity.id, entity.categories, uvsnGen(entity.body), dvsnGen(entity.body))
      case None => null
    }
  }
}