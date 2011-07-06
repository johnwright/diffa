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

/**
 * Specifies the next action to take when building a digest tree.
 */
trait QueryAction

/**
 * The next action should still be an aggregating action
 * @param bucketing the names of the bucketing functions to apply on attributes. Any attribute not named should not
 *                  be bucketed
 * @param constraints the constraints to apply to the aggregation
 */
case class AggregateQueryAction(bucketing:Seq[CategoryFunction], constraints:Seq[QueryConstraint]) extends QueryAction

/**
 * The next action should query on an individual level
 */
case class EntityQueryAction(constraints:Seq[QueryConstraint]) extends QueryAction