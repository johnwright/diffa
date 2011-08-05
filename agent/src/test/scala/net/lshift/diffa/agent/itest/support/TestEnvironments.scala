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

package net.lshift.diffa.agent.itest.support

import net.lshift.diffa.messaging.json.ChangesRestClient
import net.lshift.diffa.messaging.amqp.{AmqpQueueUrl, ConnectorHolder, ChangesAmqpClient}

/**
 * Static set of environments that can be used in integration test cases.
 */
object TestEnvironments {

  private val amqpConnectorHolder = new ConnectorHolder()

  // Each environment will need participants running on their own ports. To do this, we'll simply provide
  // a mechanism for portOffset.
  private var portOffset = 0
  private def nextPort = this.synchronized {
    portOffset += 1
    20194 + portOffset
  }

  def same(key:String) =
    new TestEnvironment(key,
                        new HttpParticipants(nextPort, nextPort),
                        { (env: TestEnvironment, epName:String) => new ChangesRestClient(env.serverRoot, env.domain.name, epName) },
                        SameVersionScheme)

  def correlated(key:String) =
    new TestEnvironment(key,
                        new HttpParticipants(nextPort, nextPort),
                        { (env: TestEnvironment, epName:String) => new ChangesRestClient(env.serverRoot, env.domain.name, epName) },
                        CorrelatedVersionScheme)

  def sameAmqp(key:String) =
    new TestEnvironment(key,
                        new HttpParticipants(nextPort, nextPort),
                        { (_, epName) => new ChangesAmqpClient(amqpConnectorHolder.connector,
                                                     epName + "-changes-same" + key,
                                                     10000) },
                        SameVersionScheme,
                        (epName:String) => AmqpQueueUrl(epName + "-changes-same" + key).toString)

  def correlatedAmqp(key:String) =
    new TestEnvironment(key,
                        new HttpParticipants(nextPort, nextPort),
                        { (_, epName:String) => new ChangesAmqpClient(amqpConnectorHolder.connector,
                                                                      epName + "-changes-correlated" + key,
                                                                      10000) },
                        CorrelatedVersionScheme,
                        (epName:String) => AmqpQueueUrl(epName + "-changes-correlated" + key).toString)
}