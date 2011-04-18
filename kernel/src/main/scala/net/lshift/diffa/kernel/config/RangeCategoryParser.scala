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

package net.lshift.diffa.kernel.config

import org.joda.time.format.ISODateTimeFormat
import net.lshift.diffa.kernel.participants.{DateTimeRangeConstraint, IntegerRangeConstraint, RangeQueryConstraint}
import net.lshift.diffa.kernel.differencing.{StringAttribute, DateAttribute, IntegerAttribute}

/**
 * This is a simple registry that can hydrate category values based on a descriptor
 */
object RangeCategoryParser {

  def parseDate(s:String) = ISODateTimeFormat.dateTimeParser.parseDateTime(s) // TODO: Force Timezone
  def parseInt(s:String) = Integer.valueOf(s).intValue

  def typedAttribute(descriptor:RangeCategoryDescriptor, value:String) = descriptor.dataType match {
    case "int"  => IntegerAttribute(parseInt(value))
    case "datetime" => DateAttribute(parseDate(value))
    case _      => StringAttribute(value)
  }

  def buildConstraint(name:String, descriptor:RangeCategoryDescriptor) = descriptor.dataType match {
    case "int"  => IntegerRangeConstraint(name, parseInt(descriptor.lower), parseInt(descriptor.upper))
    case "datetime" => DateTimeRangeConstraint(name, parseDate(descriptor.lower), parseDate(descriptor.upper))
    case _      => RangeQueryConstraint(name, descriptor.lower, descriptor.upper)
  }
}