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

import net.lshift.diffa.participant.scanning.{ScanConstraint, ScanAggregation}

/**
 * This is a struct for desceding partition requests. 
 */
case class IntermediateResult(constraint:ScanConstraint, next:CategoryFunction)

/**
 * This is a function definition that can:
 * - Given a value in a given domain, it can determine what partition that value belongs to
 * - Given the value of a partition, it can determine what the relevant upper and lower bounds are for
 *   any further introspection.
 */
trait CategoryFunction extends ScanAggregation {

  /**
   * The external name of this function.
   */
  def name : String

  /**
   * Descends into a more fine-grained partitioning mechanism.
   *
   * If this function returns None, then finer-grained partitioning is not possible.
   * This occurs for example when trying to descend using a category function for an individual entity.   
   */
  def descend : Option[CategoryFunction]

  /**
   * Given the name of a valid partition, return a query constraint that will further constrain a request to include
   * only data that exists with the partition. The parent argument is a reference to the parent constraint
   * that is used as the context within which a new constraint can be produced. Hence the returned constraint
   * is the product of the constraint that caused this function to get executed together with the current level
   * of partitioning.
   */
  def constrain(parent:Option[ScanConstraint], partition:String) : ScanConstraint

  /**
   * Indicates whether this function supports bucketing.
   */
  def shouldBucket() : Boolean
}
