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

package net.lshift.diffa.kernel.alerting

import org.slf4j.LoggerFactory

/**
 * Simple wrapper around a logger that provides for structured logging of application events (alerts) in a way
 * that can be easily inspected with log analysis tools.
 */
class Alerter(clazz:Class[_]) {
  private val log = LoggerFactory.getLogger(clazz)

  def log(key:AlertKey, ex:Throwable, msg:String):Unit = {
    key.category match {
      case InfoAlert => log.info(key.id + ": " + msg, ex)
      case ErrorAlert => log.error(key.id + ": " + msg, ex)
    }
  }

  def log(key:AlertKey, msg:String):Unit = {
    key.category match {
      case InfoAlert => log.info(key.id + ": " + msg)
      case ErrorAlert => log.error(key.id + ": " + msg)
    }
  }
}

object Alerter {
  def forClass(clazz:Class[_]) = new Alerter(clazz)
}