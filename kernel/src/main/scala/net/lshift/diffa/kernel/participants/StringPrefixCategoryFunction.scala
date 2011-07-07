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

package net.lshift.diffa.kernel.participants

import scala.util.matching.Regex
import scala.collection.JavaConversions._
import net.lshift.diffa.participant.scanning._

/**
 * Category function for partitioning on prefixes of strings.
 *
 */
case class StringPrefixCategoryFunction(attrName:String,
                                        prefixLength: Int,
                                        maxLength: Int,
                                        step: Int)
  extends StringPrefixAggregation(attrName, prefixLength)
  with CategoryFunction {

  def name = "prefix(%d,%d,%d)".format(prefixLength, maxLength, step)

  def descend =
    if (prefixLength == maxLength)
      None
    else if (prefixLength + step > maxLength)
      Some(StringPrefixCategoryFunction(attrName, maxLength, maxLength, step))
    else
      Some(StringPrefixCategoryFunction(attrName, prefixLength + step, maxLength, step))

  def constrain(partition: String) =
    if (partition.length < prefixLength)
      new SetConstraint(attrName, Set(partition))
    else if (partition.length > prefixLength)
      throw new InvalidAttributeValueException(
        "Partition value must be %d characters in length".format(prefixLength))
    else
      new StringPrefixConstraint(attrName, partition)

  val shouldBucket = true
}