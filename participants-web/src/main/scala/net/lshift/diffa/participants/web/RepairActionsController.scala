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

package net.lshift.diffa.participants.web

import org.springframework.stereotype.Controller
import net.lshift.diffa.participants.UpstreamWebParticipant
import org.springframework.web.bind.annotation.{ResponseStatus, PathVariable, RequestMethod, RequestMapping}
import org.springframework.http.HttpStatus
import java.lang.IllegalArgumentException

@Controller
@RequestMapping(Array("/actions"))
class RepairActionsController(upstream: UpstreamWebParticipant) {

  @ResponseStatus(HttpStatus.NOT_FOUND)
  @RequestMapping(value=Array("resend/{entityId}"), method=Array(RequestMethod.POST))
  def resend(@PathVariable("entityId") entityId: String) = {
    if (upstream.entityIds.contains(entityId)) {
      "json/empty"
    }
    else {
      throw new IllegalArgumentException
    }
  }

  @RequestMapping(value=Array("resend-all"), method=Array(RequestMethod.POST))
  def resendAll() = "json/empty"

}
