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

/**
 * Category function for partitioning on prefixes of strings.
 *
 */
case class StringPrefixCategoryFunction(prefixLength: Int,
                                        maxLength: Int,
                                        step: Int) extends CategoryFunction {

  def name = "prefix(%d,%d,%d)".format(prefixLength, maxLength, step)

  def descend =
    if (prefixLength == maxLength)
      Some(IndividualCategoryFunction)
    else if (prefixLength + step > maxLength)
      Some(StringPrefixCategoryFunction(maxLength, maxLength, step))
    else
      Some(StringPrefixCategoryFunction(prefixLength + step, maxLength, step))

  def constrain(categoryName: String, partition: String) =
    if (partition.length != prefixLength)
      // TODO if the partitioning value is shorter than the prefix length, return a set constraint containing that value
      throw new InvalidAttributeValueException(
        "Partition value must be %d characters in length".format(prefixLength))
    else
      PrefixQueryConstraint(categoryName, partition)

  val shouldBucket = true

  def owningPartition(value: String) =
    if (value.length < prefixLength)
      value
    else
      value.substring(0, prefixLength)
}