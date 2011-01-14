/**
 * Copyright (C) 2011 LShift Ltd.
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
 * - A well known function name to apply in the participant
 * - EITHER a list of values OR a range of values TO apply
 */
trait QueryConstraint {

  /**
   * The name of the category to constrain entities or aggregates by.
   */
  def category:String

  /**
   * The function that performs narrowing
   */
  def function:CategoryFunction

  /**
   *  Either a range or a list of values to form a constraint predicate with.
   */
  def values:Seq[String]

  /**
   * Depending on:
   *
   * - The name of a valid partition with the specified value range
   * - Whether or not a partition is being requested where one of the digest sequences is empty
   *
   * return a finer grained query to execute.
   */
  def nextQueryAction(partition:String, empty:Boolean) : Option[QueryAction]

  /**
   *  Returns a simplified representation of this constraint that is suitable for packing
   */
  def wireFormat : WireConstraint
}

abstract case class BaseQueryConstraint(category:String,function:CategoryFunction,values:Seq[String]) extends QueryConstraint {

  def nextConstraint(category:String,function:CategoryFunction,values:Seq[String]) : BaseQueryConstraint

  def nextQueryAction(partition:String, empty:Boolean) : Option[QueryAction] =
    function.descend(partition) map { intermediateResult =>
      if (empty || intermediateResult.next == IndividualCategoryFunction)
        EntityQueryAction(nextConstraint(BaseQueryConstraint.this.category, IndividualCategoryFunction, intermediateResult.toSeq))
      else
        AggregateQueryAction(nextConstraint(BaseQueryConstraint.this.category, intermediateResult.next, intermediateResult.toSeq))
    }
}

/**
 *  This type of constraint is to be interpreted as a set of values to constrain with.
 */
case class ListQueryConstraint(c:String, f:CategoryFunction, v:Seq[String]) extends BaseQueryConstraint(c,f,v) {

  def nextConstraint(category:String,function:CategoryFunction,values:Seq[String]) =  ListQueryConstraint(category,function,values)

  def wireFormat() = listConstraint(category, function, values)
}

/**
 * This type of constraint is to be interpreted as a value range - the sequence of values contains the
 * upper and lower bounds of the constraint.
 */
case class RangeQueryConstraint(c:String, f:CategoryFunction, v:Seq[String]) extends BaseQueryConstraint(c,f,v) {

  def nextConstraint(category:String,function:CategoryFunction,values:Seq[String]) =  RangeQueryConstraint(category,function,values)

  def wireFormat() = rangeConstraint(category, function, values(0), values(1))
}

abstract case class NonValueConstraint(c:String, f:CategoryFunction) extends BaseQueryConstraint(c,f,Seq()) {
  def wireFormat() = unbounded(category, function)
}

/**
 * This represents an unbounded range constraint, that, when narrowed, turns into a regular RangeQueryConstraint.
 */
case class UnboundedRangeQueryConstraint(override val c:String, override val f:CategoryFunction) extends NonValueConstraint(c,f) {
  def nextConstraint(category:String,function:CategoryFunction,values:Seq[String]) =  RangeQueryConstraint(category,function, values)
}

/**
 * This represents an unbounded constraint.
 */
case class NoConstraint(override val c:String, override val f:CategoryFunction) extends NonValueConstraint(c,f) {
  def nextConstraint(category:String,function:CategoryFunction,values:Seq[String]) =  NoConstraint(category,function)
}

/**
 *   Utility builders
 */
object EasyConstraints {
  def dateRangeConstraint(start: DateTime, end: DateTime, f: CategoryFunction) =
    RangeQueryConstraint("date", f, Seq(start.toString, end.toString))
  def intRangeConstraint(start: Int, end: Int, f: CategoryFunction) =
    RangeQueryConstraint("int", f, Seq(start.toString, end.toString))
  def unconstrainedDate(f: CategoryFunction) = UnboundedRangeQueryConstraint("date", f)
  def unconstrainedInt(f: CategoryFunction) = UnboundedRangeQueryConstraint("int", f)
}