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

package net.lshift.diffa.messaging.json

import org.joda.time.format.ISODateTimeFormat
import org.joda.time.DateTime
import org.codehaus.jettison.json.JSONArray

/**
 * Standard utilities for JSON encoding.
 */

object JSONEncodingUtils {
  val dateEncoder = ISODateTimeFormat.dateTime
  val dateParser = ISODateTimeFormat.dateTimeParser

  def maybeDateStr(date:DateTime) = {
    date match {
      case null => null
      case _    => date.toString(JSONEncodingUtils.dateEncoder)
    }
  }

  def maybeParseableDate(s:String) = {
    s match {
      case null => null
      case ""   => null
      case _    => JSONEncodingUtils.dateParser.parseDateTime(s)
    }
  }

  // TODO I can't believe you have to do this
  def toList[T](a:JSONArray) : Seq[T] = {
    if (null == a) {
      List()
    }
    else {
      for (val i <- 0 to a.length - 1 ) yield a.get(i).asInstanceOf[T]
    }
  }
}