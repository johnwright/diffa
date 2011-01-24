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

import org.joda.time.DateTime
import net.lshift.diffa.kernel.frontend.wire.WireConstraint
import net.lshift.diffa.kernel.frontend.wire.WireConstraint._
import scala.collection.Map
import scala.collection.JavaConversions._

/**
 * Contains
 *
 * - The name of the category
 * - EITHER a list of values OR a range of values TO apply
 */
trait QueryConstraint {

  /**
   * The name of the category to constrain entities or aggregates by.
   */
  def category:String

  /**
   *  Either a range or a list of values to form a constraint predicate with.
   */
  def values:Seq[String]

  /**
   *  Returns a simplified representation of this constraint that is suitable for packing
   */
  def wireFormat : WireConstraint
}

abstract case class BaseQueryConstraint(category:String,values:Seq[String]) extends QueryConstraint {
}

/**
 *  This type of constraint is to be interpreted as a set of values to constrain with.
 */
case class ListQueryConstraint(c:String, v:Seq[String]) extends BaseQueryConstraint(c,v) {
  def wireFormat() = listConstraint(category, values)
}

/**
 * This type of constraint is to be interpreted as a value range - the sequence of values contains the
 * upper and lower bounds of the constraint.
 */
case class RangeQueryConstraint(c:String, v:Seq[String]) extends BaseQueryConstraint(c,v) {
  def wireFormat() = rangeConstraint(category, values(0), values(1))
}

abstract case class NonValueConstraint(c:String) extends BaseQueryConstraint(c,Seq()) {
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
  def dateRangeConstraint(cat:String, start:DateTime, end:DateTime) = RangeQueryConstraint(cat, Seq(start.toString(), end.toString()))
  def unconstrainedDate(cat:String) = UnboundedRangeQueryConstraint(cat)
  
  def intRangeConstraint(cat:String, start: Int, end: Int) = RangeQueryConstraint(cat, Seq(start.toString, end.toString))
  def unconstrainedInt(cat:String) = UnboundedRangeQueryConstraint(cat)
}