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

import net.lshift.diffa.kernel.util.db.{HibernateQueryUtils => HQU}
import net.lshift.diffa.kernel.util.db.SessionHelper._
import org.hibernate.{Session, SessionFactory}


/**
 * Hibernate backed implementation of the low level DB facade.
 */
class HibernateDatabaseFacade(factory:SessionFactory) extends DatabaseFacade {

  def listQuery[T](queryName: String, params: Map[String, Any], firstResult: Option[Int], maxResults: Option[Int]) = {
    factory.withSession(s => HQU.listQuery(s, queryName, params, firstResult, maxResults))
  }

  def singleQuery[T](queryName: String, params: Map[String, Any], entityName: String)  = {
    factory.withSession(s => HQU.singleQuery[T](s, queryName, params, entityName))
  }

  def singleQueryMaybe[T](queryName: String, params: Map[String, Any])  = {
    factory.withSession(s => HQU.singleQueryOpt(s, queryName, params))
  }

  def execute(queryName: String, params: Map[String, Any]) : Int = {
    factory.withSession(s => HQU.executeUpdate(s, queryName, params))
  }

  def insert[T](o:T) : T  = {
    factory.withSession(s => {
      s.save(o)
      o
    })
  }

  def beginTransaction = new SessionBasedTransaction(factory)

}

class SessionBasedTransaction(factory:SessionFactory) extends Transaction {

  val session = factory.openSession()
  val tx = session.beginTransaction()


  def singleQueryMaybe[T](command:DatabaseCommand) = executeInternal(s => {
    HQU.singleQueryOpt(s, command.queryName, command.params)
  })

  def execute(command:DatabaseCommand) = executeInternal(s => {
    Some(HQU.executeUpdate(s, command.queryName, command.params))
  }).get

  def commit() {
    try {
      if (tx != null && tx.isActive) {
        tx.commit()
      }
      else {
        throw new RuntimeException("Invalid transation state")
      }
    }
    finally {
      session.close()
    }
  }

  private def executeInternal[T](hqu:Session => Option[T]) = {
    try {
      hqu(session)
    }
    catch {
      case x:Exception => {
        if (tx != null) {
          try {
            tx.rollback()
          }
          finally {
            session.close()
          }

        }
        else {
          session.close()
        }
        throw x
      }
    }
  }
}
