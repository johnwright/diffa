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

package net.lshift.diffa.kernel.frontend

import net.lshift.diffa.kernel.participants._
import scala.collection.JavaConversions._

/**
 * Provides a simple registry so that constraints in wire formats can
 * easily get mapped back to their kernel counterparts.
 */
object ConstraintRegistry {

  val registry = Map(
    IndividualCategoryFunction.name -> IndividualCategoryFunction,
    DailyCategoryFunction.name -> DailyCategoryFunction,
    MonthlyCategoryFunction.name -> MonthlyCategoryFunction,
    YearlyCategoryFunction.name -> YearlyCategoryFunction
  )

  def resolve(wire:WireConstraint) : QueryConstraint = {
    wire.validate
    if (wire.values != null) {
      ListQueryConstraint(wire.dataType, resolveConstraint(wire), wire.values)
    }
    else {
      val lower = wire.attibutes.get(WireConstraint.LO)
      val upper = wire.attibutes.get(WireConstraint.HI)
      if (lower != null && upper != null) {
        RangeQueryConstraint(wire.dataType, resolveConstraint(wire), Seq(lower,upper))
      }
      else {
        UnboundedRangeQueryConstraint(wire.dataType, resolveConstraint(wire))
      }
    }
  }

  private def resolveConstraint(wire:WireConstraint) : CategoryFunction = {
    val functionName = wire.attibutes.get(WireConstraint.FUN)
    registry(functionName)
  }
}