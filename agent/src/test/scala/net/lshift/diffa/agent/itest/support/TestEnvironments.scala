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

import net.lshift.diffa.messaging.json.ChangesRestClient
import net.lshift.diffa.messaging.amqp.{ConnectorHolder, ChangesAmqpClient}

/**
 * Static set of environments that can be used in integration test cases.
 */
object TestEnvironments {

  private val amqpConnectorHolder = new ConnectorHolder()

  lazy val abSame =
    new TestEnvironment("abSame",
                        new HttpParticipants(20094, 20095),
                        { env: TestEnvironment => new ChangesRestClient(env.serverRoot) },
                        SameVersionScheme)

  lazy val abCorrelated =
    new TestEnvironment("abCorrelated",
                        new HttpParticipants(20194, 20195),
                        { env: TestEnvironment => new ChangesRestClient(env.serverRoot) },
                        CorrelatedVersionScheme)

  lazy val abSameAmqp =
    new TestEnvironment("abSameAmqp",
                        new AmqpParticipants(amqpConnectorHolder,
                                             "participant-us-same",
                                             "participant-ds-same",
                                             "changes-same"),
                        { _ => new ChangesAmqpClient(amqpConnectorHolder.connector,
                                                     "changes-same",
                                                     10000) },
                        SameVersionScheme)

  lazy val abCorrelatedAmqp =
    new TestEnvironment("abCorrelatedAmqp",
                        new AmqpParticipants(amqpConnectorHolder,
                                             "participant-us-correlated",
                                             "participant-ds-correlated",
                                             "changes-correlated"),
                        { _ => new ChangesAmqpClient(amqpConnectorHolder.connector,
                                                     "changes-correlated",
                                                     10000) },
                        CorrelatedVersionScheme)
}