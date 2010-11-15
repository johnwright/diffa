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
  def category:String
  def function:CategoryFunction
  def values:Seq[String]
  def nextQueryAction(partition:String, empty:Boolean) : Option[QueryAction]
}

abstract case class BaseQueryConstraint(category:String,function:CategoryFunction,values:Seq[String]) extends QueryConstraint {

  def nextConstraint(category:String,function:CategoryFunction,values:Seq[String]) : BaseQueryConstraint

  // TODO [#2] unit test
  def nextQueryAction(partition:String, empty:Boolean) : Option[QueryAction] = {
    function.evaluate(partition) match {
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
case class ListQueryConstraint(c:String, f:CategoryFunction, v:Seq[String]) extends BaseQueryConstraint(c,f,v) {
  def nextConstraint(category:String,function:CategoryFunction,values:Seq[String]) =  ListQueryConstraint(category,function,values)
}
case class RangeQueryConstraint(c:String, f:CategoryFunction, v:Seq[String]) extends BaseQueryConstraint(c,f,v) {
  def nextConstraint(category:String,function:CategoryFunction,values:Seq[String]) =  RangeQueryConstraint(category,function,values)
}
case class NoConstraint(c:String, f:CategoryFunction) extends BaseQueryConstraint(c,f,Seq()) {
  def nextConstraint(category:String,function:CategoryFunction,values:Seq[String]) =  NoConstraint(category,function)
}