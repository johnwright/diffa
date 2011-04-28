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

import net.lshift.diffa.kernel.participants.{DateRangeConstraint, DateTimeRangeConstraint, IntegerRangeConstraint, RangeQueryConstraint}
import org.joda.time.format.{DateTimeFormatterBuilder, ISODateTimeFormat}
import org.slf4j.LoggerFactory
import net.lshift.diffa.kernel.differencing.{DateAttribute, StringAttribute, DateTimeAttribute, IntegerAttribute}

/**
 * This is a simple registry that can hydrate category values based on a descriptor
 */
object RangeCategoryParser {

  val log = LoggerFactory.getLogger(getClass)

  val dateParsers = Array(ISODateTimeFormat.date().getParser)
  protected val dateFormatter = new DateTimeFormatterBuilder().append( null, dateParsers ).toFormatter

  def parseDateTime(s:String) = ISODateTimeFormat.dateTimeParser.parseDateTime(s) // TODO: Force Timezone
  def parseDate(s:String) = parseDateTime(s).toLocalDate
  def parseInt(s:String) = Integer.valueOf(s).intValue

  def typedAttribute(descriptor:RangeCategoryDescriptor, value:String) = descriptor.dataType match {
    case "int"      => IntegerAttribute(parseInt(value))
    case "date"     => DateAttribute(parseDate(value))
    case "datetime" => DateTimeAttribute(parseDateTime(value))
    case t          =>
      log.warn("Casting value %s (which has the configured type %s) to a string".format(value,t))
      StringAttribute(value)
  }

  // TODO Timezone handling may not be quite correct
  def buildConstraint(name:String, descriptor:RangeCategoryDescriptor) = descriptor.dataType match {
    case "int"      => IntegerRangeConstraint(name, parseInt(descriptor.lower), parseInt(descriptor.upper))
    case "datetime" => {
      try {
        // Attempt to parse a yyyy-MM-dd format and widen
        val lower = dateFormatter.parseDateTime(descriptor.lower)
        val upper = dateFormatter.parseDateTime(descriptor.upper).plusDays(1).minusMillis(1)
        DateTimeRangeConstraint(name, lower, upper)
      }
      catch {
        // The format is not yyyy-MM-dd, so don't widen
        case e:IllegalArgumentException =>
          DateTimeRangeConstraint(name, parseDateTime(descriptor.lower), parseDateTime(descriptor.upper))
      }
    }
    case "date"     => DateRangeConstraint(name, parseDate(descriptor.lower), parseDate(descriptor.upper))
    case _          => RangeQueryConstraint(name, descriptor.lower, descriptor.upper)
  }
}