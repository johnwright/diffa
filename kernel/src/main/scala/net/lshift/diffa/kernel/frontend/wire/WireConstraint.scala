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
  @BeanProperty var category:String,
  @BeanProperty var attributes:Map[String,String],
  @BeanProperty var values:List[String]) {

  import WireConstraint.{LO, HI, PREFIX}

  def this() = this(null,null,null)

  /**
   *  Simple validation function - the wire format is not constained by any schema ATM
   */
  def validate() = {
    if (category == null) {
      throw new InvalidWireConstraint(this, "missing datatype")
    }
    if (attributes == null) {
      throw new InvalidWireConstraint(this, "missing attributes")
    }
    if (values != null) {
      if (attributes.containsKey(LO) || attributes.containsKey(HI)) {
        throw new InvalidWireConstraint(this, "contains values AND range")
      }
    }
    else if (((!attributes.containsKey(LO) && attributes.containsKey(HI)))) {
        throw new InvalidWireConstraint(this, "incomplete bounds")
    }
  }

  def toQueryConstraint: QueryConstraint = {
    validate
    if (values != null) {
      SetQueryConstraint(category, values.toSet)
    } else if (attributes.containsKey(PREFIX)) {
      PrefixQueryConstraint(category, attributes.get(PREFIX))
    } else if (attributes.containsKey(LO) && attributes.containsKey(HI)) {
      val lower = attributes.get(LO)
      val upper = attributes.get(HI)
      if (lower != null && upper != null) {
        RangeQueryConstraint(category, lower, upper)
      } else {
        UnboundedRangeQueryConstraint(category)
      }
    } else {
      UnboundedRangeQueryConstraint(category)
    }
  }
}

class InvalidWireConstraint(wire:WireConstraint, s:String) extends Exception(s + ": " + wire)

object WireConstraint {
  val LO = "lower"
  val HI = "upper"
  val PREFIX = "prefix"

  def rangeConstraint(category:String, lower:AnyRef, upper:AnyRef) = {
    WireConstraint(category, scala.collection.Map(LO -> lower.toString(), HI -> upper.toString()), null)
  }

  def setConstraint(category:String, values:Set[String]) = {
    WireConstraint(category, new java.util.HashMap, values.toList)
  }

  def unbounded(category:String) = {
    WireConstraint(category, new java.util.HashMap, null)
  }

  def prefixConstraint(category: String, prefix: String) = {
    WireConstraint(category, scala.collection.Map(PREFIX -> prefix), null)
  }
}