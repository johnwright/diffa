/**
 * Copyright (C) 2012 LShift Ltd.
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

import net.lshift.diffa.agent.client.DifferencesRestClient
import org.joda.time.DateTime
import net.lshift.diffa.kernel.differencing.DifferenceEvent
import org.junit.Assert._

/**
 * Helper class for retrieving differences.
 */
class DifferencesHelper(pairKey:String, diffClient:DifferencesRestClient) {
  def pollForAllDifferences(from:DateTime, until:DateTime, n:Int = 20, wait:Int = 100, minLength:Int = 1) =
    tryAgain((d:DifferencesRestClient) => d.getEvents(pairKey, from, until, 0, 100) ,n,wait,minLength)

  def tryAgain(poll:DifferencesRestClient => Seq[DifferenceEvent], n:Int = 20, wait:Int = 100, minLength:Int = 1) : Seq[DifferenceEvent]= {
    var i = n
    var diffs = poll(diffClient)
    while(diffs.length < minLength && i > 0) {
      Thread.sleep(wait)

      diffs = poll(diffClient)
      i-=1
    }
    assertNotNull(diffs)
    diffs
  }

  def waitForDiffCount(from:DateTime, until:DateTime, requiredCount:Int, n:Int = 20, wait:Int = 100) = {
    def poll() = diffClient.getEvents(pairKey, from, until, 0, 100)

    var i = n
    var diffs = poll()
    while(diffs.length != requiredCount && i > 0) {
      Thread.sleep(wait)

      diffs = poll()
      i-=1
    }


    if (diffs.length != requiredCount) {
      throw new Exception(
        "Never reached required diff count %s. Last attempt returned %s diffs".format(requiredCount, diffs.length))
    }

    diffs
  }
}