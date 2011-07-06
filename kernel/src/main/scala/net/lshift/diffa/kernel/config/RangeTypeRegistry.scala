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

package net.lshift.diffa.kernel.config

import net.lshift.diffa.kernel.participants.{CategoryFunction, IntegerCategoryFunction, EasyConstraints, YearlyCategoryFunction}

/**
 * Simple registry to be allow to dispatching on RangeCategoryDescriptors by their data type.
 *
 * ATM this is not as strongly typed as it could be because the type is persisted as a string, but at least this
 * lookup is centralized.
 */
object RangeTypeRegistry {

  /**
   * Resolve the default category function for any given data type name
   */
  def defaultCategoryFunction(attrName:String, dataType:String):CategoryFunction = dataType match {
    case "date"     => YearlyCategoryFunction(attrName)
    case "datetime" => YearlyCategoryFunction(attrName)
    case "int"      => IntegerCategoryFunction(attrName, 1000, 10)
  }

  /**
   * Resolve a typed unbound constraint for any given data type name
   */
  def unboundedConstraint(dataType:String, name:String) = dataType match {
    case "date"     => EasyConstraints.unconstrainedDate(name)
    case "datetime" => EasyConstraints.unconstrainedDateTime(name)
    case "int"      => EasyConstraints.unconstrainedInt(name)
  }


}