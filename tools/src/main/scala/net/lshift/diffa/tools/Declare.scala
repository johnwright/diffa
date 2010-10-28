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

package net.lshift.diffa.tools

import client.ConfigurationRestClient
import org.apache.commons.cli.{CommandLine, Option}
import net.lshift.diffa.kernel.client.ConfigurationClient

/**
 * Utility class for declaring a differencing configuration.
 */
object Declare extends DiffaTool {
  options.addOption(new Option("pairKey", true, "the key of the pair to declare"))
  options.addOption(new Option("pairGroup", true, "the group that the pair belongs to"))
  options.addOption(new Option("upstreamName", true, "the name of the upstream participant"))
  options.addOption(new Option("upstreamUrl", true, "the url of the upstream participant"))
  options.addOption(new Option("downstreamName", true, "the name of the downstream participant"))
  options.addOption(new Option("downstreamUrl", true, "the url of the downstream participant"))
  options.addOption(new Option("versionPolicy", true, "the version policy (same or correlated) to use"))
  options.addOption(new Option("matchTimeout", true, "timeout before raising matching alerts"))

  // TODO This should really be passed through from the CLI, but ATM the only serialization
  // Diffa supports is JSOn anyway
  val contentType = "application/json"
  val inboundUrl = "changes"

  def run(line:CommandLine, agentUrl:String) {
    val configClient:ConfigurationClient = new ConfigurationRestClient(agentUrl)
    var hasDeclared = false

    // Try declaring the group
    if (line.hasOption("pairGroup")) {
      val group = line.getOptionValue("pairGroup")

      println("Declaring group: " + group)
      configClient.declareGroup(group)
      hasDeclared = true
    }

    // Try declaring participants
    if (tryDeclareParticipant(line, configClient, "upstreamName", "upstreamUrl")) hasDeclared = true
    if (tryDeclareParticipant(line, configClient, "downstreamName", "downstreamUrl")) hasDeclared = true

    // Try declaring the pair
    if (hasAllOptions(line, "pairGroup", "pairKey", "versionPolicy", "upstreamName", "downstreamName")) {
      val group = line.getOptionValue("pairGroup")
      val pairKey = line.getOptionValue("pairKey")
      val versionPolicy = line.getOptionValue("versionPolicy")
      val upstreamName = line.getOptionValue("upstreamName")
      val downstreamName = line.getOptionValue("downstreamName")
      val matchTimeout = if (line.hasOption("matchTimeout")) {
          Integer.parseInt(line.getOptionValue("matchTimeout"))
        } else {
          0
        }

      println("Declaring pair: " + group + "." + pairKey + " -> (" + upstreamName + " <= {" + versionPolicy + "} => " + downstreamName + ")")
      configClient.declarePair(pairKey, versionPolicy, matchTimeout, upstreamName, downstreamName, group)
      hasDeclared = true
    }

    if (!hasDeclared) {
      println("Not enough options to declare anything")
      printUsage
      System.exit(1)
    }
  }

  protected def tryDeclareParticipant(line:CommandLine, configClient:ConfigurationClient, nameKey:String, urlKey:String) = {
    if (hasAllOptions(line, nameKey, urlKey)) {
      val name = line.getOptionValue(nameKey)
      val url = line.getOptionValue(urlKey)

      println("Declaring endpoint: " + name + " -> " + url)
      configClient.declareEndpoint(name, url, contentType, inboundUrl, true)
      true
    } else {
      false
    }
  }

  protected def hasAllOptions(line:CommandLine, names:String*) = names.forall(line.hasOption(_))
}