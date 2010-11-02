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

package net.lshift.diffa.kernel.participants

/**
 * Contains
 *
 * - The name of the category
 * - A well known function name to apply in the participant
 * - EITHER a list of values OR a range of values TO apply
 */
trait QueryConstraint {
  def category:String
  def function:CategoryFunction
  def values:Seq[String]
}
case class ListQueryConstraint(category:String, function:CategoryFunction, values:Seq[String]) extends QueryConstraint
trait RangeQueryConstraint extends QueryConstraint