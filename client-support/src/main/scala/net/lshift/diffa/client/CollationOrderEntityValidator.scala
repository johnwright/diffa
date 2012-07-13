/**
 * Copyright (C) 2010-2012 LShift Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.lshift.diffa.client

import net.lshift.diffa.participant.scanning.{ScanResultEntry, Collation, OutOfOrderException}
import java.io.InputStream
import net.lshift.diffa.participant.common.ScanEntityValidator
import net.lshift.diffa.participant.changes.ChangeEvent


class CollationOrderEntityValidator(collation: Collation) extends ScanEntityValidator {
  private var previous: Option[ScanResultEntry] = None

   override def process(current: ScanResultEntry) = {
     println("%s.process; previous:%s; current:%s".format(getClass, previous, current))
     previous.foreach { prev =>
       var res = collation.sortsBefore(current.getId, prev.getId)

       println("%s.sortsBefore(%s, %s) => %s".format(collation, current.getId, prev.getId, res))
       if(res) {
        throw new OutOfOrderException(current.getId, prev.getId)
       }
     }
     previous = Some(current)
  }

  override def process(c: ChangeEvent) = ()
}