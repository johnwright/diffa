/**
 * Copyright (C) 2010-2012 LShift Ltd.
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
package net.lshift.diffa.kernel.scanning

import org.joda.time.DateTime
import net.lshift.diffa.kernel.config.DiffaPairRef

/**
 * Provides access to reading and writing scan statements
 */
trait ScanActivityStore {

  def createOrUpdateStatement(s:ScanStatement)
  def getStatement(pair:DiffaPairRef, id:Long) : ScanStatement

}

case class ScanStatement(id:Long,
                         domain:String,
                         pair:String,
                         initiatedBy:String,
                         startTime:DateTime,
                         endTime:DateTime = null,
                         state:Int = 0)
