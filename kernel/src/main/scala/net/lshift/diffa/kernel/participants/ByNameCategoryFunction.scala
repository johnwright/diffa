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

import net.lshift.diffa.participant.scanning.{SetConstraint, ScanConstraint, ByNameAggregation}
import scala.collection.JavaConversions._

/**
 * This function partitions based on a set of attribute names who individually form single level buckets.
 */
case class ByNameCategoryFunction(attrName:String)
  extends ByNameAggregation(attrName)
  with CategoryFunction {

  def name = "by-name"
  def shouldBucket() = true
  def descend = None
  def constrain(partition: String) = new SetConstraint(attrName, Set(partition))
}