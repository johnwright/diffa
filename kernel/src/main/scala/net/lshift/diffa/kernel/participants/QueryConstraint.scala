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

import org.joda.time.DateTime

/**
 * Contains
 *
 * - The name of the category
 * - A well known function name to apply in the participant
 * - EITHER a list of values OR a range of values TO apply
 */
trait QueryConstraint {

  /**
   * The name of the catgory to constrain entities or aggregates by.
   */
  def category:String

  /**
   * The function that peforms narrowing
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
   * - Whether or not a parition is being requested where one of the digest sequences is empty
   *
   * return a finer grained query to execute.
   */
  def nextQueryAction(partition:String, empty:Boolean) : Option[QueryAction]
}

abstract case class BaseQueryConstraint(category:String,function:CategoryFunction,values:Seq[String]) extends QueryConstraint {

  def nextConstraint(category:String,function:CategoryFunction,values:Seq[String]) : BaseQueryConstraint

  def nextQueryAction(partition:String, empty:Boolean) : Option[QueryAction] = {
    function.descend(partition) match {
      case None    => None
      case Some(x) => {
        if (empty || x.next.isInstanceOf[IndividualCategoryFunction]) {          
          Some(EntityQueryAction(nextConstraint(BaseQueryConstraint.this.category, IndividualCategoryFunction(), x.toSeq)))
        }
        else {
          Some(AggregateQueryAction(nextConstraint(BaseQueryConstraint.this.category, x.next, x.toSeq)))
        }
      }
    }
  }  
}

/**
 * This type of constraint is to be interpreted as a set of values to constain with.
 */
case class ListQueryConstraint(c:String, f:CategoryFunction, v:Seq[String]) extends BaseQueryConstraint(c,f,v) {
  def nextConstraint(category:String,function:CategoryFunction,values:Seq[String]) =  ListQueryConstraint(category,function,values)
}

/**
 * This type of constraint is to be interpreted as a value range - the sequence of values contains the
 * upper and lower bounds of the constraint.
 */
case class RangeQueryConstraint(c:String, f:CategoryFunction, v:Seq[String]) extends BaseQueryConstraint(c,f,v) {
  def nextConstraint(category:String,function:CategoryFunction,values:Seq[String]) =  RangeQueryConstraint(category,function,values)
}

/**
 * This represents an unbounded constraint.
 */
case class NoConstraint(c:String, f:CategoryFunction) extends BaseQueryConstraint(c,f,Seq()) {
  def nextConstraint(category:String,function:CategoryFunction,values:Seq[String]) =  NoConstraint(category,function)
}

/**
 *   Utility builders
 */
object EasyConstraints {
  def dateRangeConstaint(start:DateTime, end:DateTime, f:CategoryFunction) = RangeQueryConstraint("date", f, Seq(start.toString(), end.toString()))
}