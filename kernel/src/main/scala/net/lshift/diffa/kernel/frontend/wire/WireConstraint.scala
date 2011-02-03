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

package net.lshift.diffa.kernel.frontend.wire

import java.util.Map
import java.util.List
import reflect.BeanProperty
import net.lshift.diffa.kernel.participants._
import scala.collection.JavaConversions._

/**
 * This is a structure that is straightforward to pack and unpack onto and off a wire.
 */
case class WireConstraint(
  @BeanProperty var dataType:String,
  @BeanProperty var attributes:Map[String,String],
  @BeanProperty var values:List[String]) {
  
  def this() = this(null,null,null)

  /**
   *  Simple validation function - the wire format is not constained by any schema ATM
   */
  def validate() = {
    if (dataType == null) {
      throw new InvalidWireConstraint(this, "missing datatype")
    }
    if (attributes == null) {
      throw new InvalidWireConstraint(this, "missing attributes")
    }
    if (values != null) {
      if (attributes.containsKey(WireConstraint.LO) || attributes.containsKey(WireConstraint.HI)) {
        throw new InvalidWireConstraint(this, "contains values AND range")
      }
    }
    else if (((!attributes.containsKey(WireConstraint.LO) && attributes.containsKey(WireConstraint.HI)))) {
        throw new InvalidWireConstraint(this, "incomplete bounds")
    }
  }

  def toQueryConstraint = {
    validate
    if (values != null) {
      SetQueryConstraint(dataType, values.toSet)
    }
    else {
      val lower = attributes.get(WireConstraint.LO)
      val upper = attributes.get(WireConstraint.HI)
      if (lower != null && upper != null) {
        RangeQueryConstraint(dataType, lower, upper)
      }
      else {
        UnboundedRangeQueryConstraint(dataType)
      }
    }
  }
}

class InvalidWireConstraint(wire:WireConstraint, s:String) extends Exception(s + ": " + wire)

object WireConstraint {
  val LO = "lower"
  val HI = "upper"

  def rangeConstraint(dataType:String, lower:AnyRef, upper:AnyRef) = {
    WireConstraint(dataType, scala.collection.Map(LO -> lower.toString(), HI -> upper.toString()), null)
  }

  def setConstraint(dataType:String, values:Set[String]) = {
    WireConstraint(dataType, new java.util.HashMap, values.toList)
  }

  def unbounded(dataType:String) = {
    WireConstraint(dataType, new java.util.HashMap, null)
  }  
}