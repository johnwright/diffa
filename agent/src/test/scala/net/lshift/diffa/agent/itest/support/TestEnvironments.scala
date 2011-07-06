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
                        { env: TestEnvironment => new ChangesRestClient(env.serverRoot) },
                        SameVersionScheme)

  def correlated(key:String) =
    new TestEnvironment(key,
                        new HttpParticipants(nextPort, nextPort),
                        { env: TestEnvironment => new ChangesRestClient(env.serverRoot) },
                        CorrelatedVersionScheme)

  def sameAmqp(key:String) =
    new TestEnvironment(key,
                        new HttpParticipants(nextPort, nextPort) {
                          override val inboundUrl:String = AmqpQueueUrl("changes-same" + key).toString
                        },
                        { _ => new ChangesAmqpClient(amqpConnectorHolder.connector,
                                                     "changes-same" + key,
                                                     10000) },
                        SameVersionScheme)

  def correlatedAmqp(key:String) =
    new TestEnvironment(key,
                        new HttpParticipants(nextPort, nextPort) {
                          override val inboundUrl:String = AmqpQueueUrl("changes-correlated" + key).toString
                        },
                        { _ => new ChangesAmqpClient(amqpConnectorHolder.connector,
                                                     "changes-correlated" + key,
                                                     10000) },
                        CorrelatedVersionScheme)
}