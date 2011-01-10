/**
 * Copyright (C) 2011 LShift Ltd.
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

package net.lshift.diffa.util

import net.lshift.diffa.kernel.config.PairDef
import net.lshift.diffa.tools.client.ConfigurationRestClient

/**
 * Subclass of ConfigurationRestClient specifically for use by Spring.
 * declarePairSpring is mostly similar to declarePair, except the final categories parameter accepts a java.util.SortedMap
 * which is the type it will receive when the map is defined in Spring's config.
 */
class SpringConfigurationRestClient(serverRootUrl: String) extends ConfigurationRestClient(serverRootUrl) {

  def declarePairSpring(pairKey: String, versionPolicyName: String, matchingTimeout:Int, upstreamName: String,
                  downstreamName: String, groupKey: String, categories:java.util.SortedMap[String,String]) = {
    val p = new PairDef(pairKey, versionPolicyName, matchingTimeout, upstreamName, downstreamName, groupKey, categories)
    create("pairs", p)
    p
  }

}