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

import org.apache.commons.cli._

/**
 * Base class inherited by diffa tools.
 */
abstract class DiffaTool {
  val options = new Options
  options.addOption(new Option("help", "prints this message"))
  options.addOption(new Option("agent", true, "the url of the diffa agent to use"))

  def main(args:Array[String]) {
    val parser = new PosixParser
    val line = try {
      parser.parse(options, args)
    } catch {
      case e:ParseException => {
        println(e.getMessage)
        printUsage
        System.exit(1)

        null
      }
    }

    val agentUrl = optionOrUsage(line, "agent")
    run(line, agentUrl)
  }

  protected def run(line:CommandLine, agentUrl:String)

  protected def optionOrUsage(line:CommandLine, name:String):String = {
    if (line.hasOption(name)) {
      line.getOptionValue(name)
    } else {
      printUsage
      System.exit(1)
      null
    }
  }
  protected def printUsage {
    val formatter = new HelpFormatter()
    formatter.printHelp("java -jar diffa-tools.jar", options)
  }
}