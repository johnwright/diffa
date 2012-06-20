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

package net.lshift.diffa.kernel.differencing

import net.lshift.diffa.participant.scanning.ScanResultEntry
import org.joda.time.DateTime
import net.lshift.diffa.participant.common.{InvalidEntityException, ScanResultEntryValidator}


case class ValidatableEntity(id:String, version:String, lastUpdated:DateTime, attributes: Map[String, String]) {

}

object EntityValidator extends ScanResultEntryValidator {
  import scala.collection.JavaConversions._
  def maybe[T](x: T) = x match {
    case null => None
    case x => Some(x)
  }

  def validateCharactersIn(s: String) = {
    // println("Validate chars: " + string)
    if (!java.util.regex.Pattern.compile("^\\p{Graph}*$").matcher(s).matches())
      throw new InvalidEntityException(s)
  }
  def validate(e: ValidatableEntity): Unit = {
    // println("Validating: %s".format(this))
    if (e.id != null) validateCharactersIn(e.id)
    e.attributes.foreach { case (_, value) => validateCharactersIn(value) }
  }

  def process(e: ScanResultEntry): Unit = validate(of(e))

  private def of(e: ScanResultEntry) = ValidatableEntity(e.getId, e.getVersion, e.getLastUpdated,
    maybe(e.getAttributes).map(_.toMap).getOrElse(Map()))


}