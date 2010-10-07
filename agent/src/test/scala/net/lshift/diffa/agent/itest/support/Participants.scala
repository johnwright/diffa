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

package net.lshift.diffa.agent.itest.support

import net.lshift.diffa.kernel.protocol.ProtocolHandler
import net.lshift.diffa.kernel.participants.{DownstreamParticipant, UpstreamParticipant}
import net.lshift.diffa.participants.ParticipantRpcServer
import concurrent.SyncVar
import net.lshift.diffa.messaging.json.{DownstreamParticipantHandler, UpstreamParticipantHandler}

/**
 * Helper object for creation of HTTP/protobuf RPC chain for remote-controlling participants
 */
object Participants {
  def startUpstreamServer(port:Int, upstream:UpstreamParticipant) =
    forkServer(port, new UpstreamParticipantHandler(upstream))

  def startDownstreamServer(port:Int, downstream:DownstreamParticipant) =
    forkServer(port, new DownstreamParticipantHandler(downstream))
  

  private def forkServer(port:Int, handler:ProtocolHandler):Unit = {
    val server = new ParticipantRpcServer(port, handler)
    val startupSync = new SyncVar[Boolean]
    new Thread {
      override def run = {
        server.start
        startupSync.set(true)
      }
    }.start

    startupSync.get(5000) match {
      case None => throw new Exception("Forked server on " + port + " failed to start")
      case _    =>
    }
  }
}