/**
 * Copyright (C) 2010-2012 LShift Ltd.
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

package net.lshift.diffa.kernel.util.db

import org.hibernate.Session
import net.lshift.diffa.kernel.util.MissingObjectException

/**
 * Abstraction that hopefully lends itself to multiple implementations and allows DiffStore implementations
 * to be slightly less coupled to the underlying database, in terms of access and caching.
 */
trait DatabaseFacade {

  /**
   * Executes the named SELECT query with the specified parameters and binds the results to the expected type.
   */
  def listQuery[T](queryName: String,
                   params: Map[String, Any],
                   firstResult:Option[Int] = None,
                   maxResults:Option[Int] = None): Seq[T]

  /**
   * Executes the named SELECT query for a single row with the specified parameters and binds the results to the expected type.
   * @throws MissingObjectException
   */
  def singleQuery[T](queryName: String,
                     params: Map[String, Any],
                     entityName: String) : T

  def singleQueryMaybe[T](queryName: String,
                          params: Map[String, Any]) : Option[T]

  /**
   * Executes the named UPDATE, DELETE or INSERT query with the specified parameters and the row count
   */
  def execute(queryName: String, params: Map[String, Any]) : Int

  /**
   * This is a thin wrapper around the Hibernate Session save/1 call.
   * Ideally, we don't want to use this at all, since Hibernate will issue a SELECT to get the new primary back,
   * hence costing us an extra network round trip. However, until we can replace the current key generation wiring
   * in Hibernate, we're going to have to remain dependent on Hibernate.
   */
  @Deprecated def insert[T](o:T) : T

  def beginTransaction : Transaction
}

case class DatabaseCommand(
  queryName:String,
  params:Map[String, Any]
)

trait Transaction {

  def singleQueryMaybe[T](command:DatabaseCommand) : Option[T]
  def execute(command:DatabaseCommand) : Int
  @Deprecated def insert[T](o:T) : T
  def commit()
  def registerRollbackHandler(h:RollbackHandler)

}

trait RollbackHandler {
  def onRollback()
}

