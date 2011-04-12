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

import net.lshift.diffa.kernel.frontend.wire.WireConstraint
import net.lshift.diffa.kernel.frontend.wire.WireConstraint._
import scala.collection.Map
import scala.collection.JavaConversions._

/**
 * Base type for all query constraints. Enforces that all constraints define the category they are constraining and
 * an ability to transform to a WireConstraint. Implementations will define any additional parameters to be applied
 * to the constraint.
 */
trait QueryConstraint {

  /**
   * The name of the category to constrain entities or aggregates by.
   */
  def category:String

  /**
   *  Returns a simplified representation of this constraint that is suitable for packing
   */
  def wireFormat : WireConstraint

  /**
   * This allows a concrete constraint to define how it would group itself into multiple batches for
   * convenient transmission over the wire.
   */
  def group : Seq[QueryConstraint] = Seq(this)
}

abstract case class BaseQueryConstraint(category:String) extends QueryConstraint

/**
 *  This type of constraint is to be interpreted as a set of values to constrain with.
 */
case class SetQueryConstraint(c:String, values:Set[String]) extends BaseQueryConstraint(c) {
  def wireFormat() = setConstraint(category, values)
  override def group = values.map(v => SetQueryConstraint(c,Set(v))).toSeq
}

/**
 * This type of constraint is to be interpreted as a value range - the sequence of values contains the
 * upper and lower bounds of the constraint.
 */
case class RangeQueryConstraint(c:String, lower:String, upper:String) extends BaseQueryConstraint(c) {
  def wireFormat() = rangeConstraint(category, lower, upper)
}

case class PrefixQueryConstraint(c: String, prefix: String) extends BaseQueryConstraint(c) {
  def wireFormat() = prefixConstraint(c, prefix)
}

abstract case class NonValueConstraint(c:String) extends BaseQueryConstraint(c) {
  def wireFormat() = unbounded(category)
}

/**
 * This represents an unbounded range constraint, that, when narrowed, turns into a regular RangeQueryConstraint.
 */
case class UnboundedRangeQueryConstraint(override val c:String) extends NonValueConstraint(c) {
}

/**
 * This represents an unbounded constraint.
 */
case class NoConstraint(override val c:String) extends NonValueConstraint(c) {
}

/**
 *   Utility builders
 */
object EasyConstraints {
  def unconstrainedDate(cat:String) = UnboundedRangeQueryConstraint(cat)

  def unconstrainedInt(cat:String) = UnboundedRangeQueryConstraint(cat)
}