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

package net.lshift.diffa.tools

import client.DifferencesRestClient
import org.joda.time.DateTime
import java.lang.String
import org.apache.commons.cli._
import net.lshift.diffa.kernel.client.DifferencesClient
import net.lshift.diffa.kernel.differencing.{SessionScope, MatchState}

/**
 * Simple utility class for tailing differences that are appearing to a console. 
 */
object DiffTail extends DiffaTool {
  options.addOption(new Option("pairKey", true, "the key of the pair to difference"))

  protected def run(line: CommandLine, agentUrl: String) = {
    val until = new DateTime
    val from = until.minusYears(1)
    val pairKey = optionOrUsage(line, "pairKey")

    val diffClient:DifferencesClient = new DifferencesRestClient(agentUrl)
    val uri = diffClient.subscribe(SessionScope.forPairs(pairKey))

    while (true) {
      diffClient.pollLoop(uri)(d => {
        d.state match {
          // TODO Consider adding a -v option to the CLI to print out verbose mismatches
          // In this scenario you would query the REST interface for the details of a mismatch
          // e.g. to give the upstream and downstream hashes
          case MatchState.MATCHED   => println( "> Alignment:  " + d.objId )
          case MatchState.UNMATCHED => println( "> Difference: " + d.objId + " (" + d.seqId + ")")
        }
      })
    }   
  }
}