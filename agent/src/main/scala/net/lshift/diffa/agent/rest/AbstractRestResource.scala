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

package net.lshift.diffa.agent.rest

import javax.ws.rs.WebApplicationException
import javax.ws.rs.core.{Context, UriInfo, Response}
import org.slf4j.{Logger, LoggerFactory}
import net.lshift.diffa.kernel.util.MissingObjectException

abstract class AbstractRestResource {

  private val log:Logger = LoggerFactory.getLogger(getClass)

  @Context var uriInfo:UriInfo = null

  def maybe[T](f: String => T, key:String) = {
    try {
      f(key)
    }
    catch {
      case e:MissingObjectException => {
        log.debug("Attempt to perform an operation with a non-existent key:" + key)
        throw new WebApplicationException(404)
      }
    }
  }

  def maybe[T](f: Seq[String] => T, keys:Seq[String]) = {
    try {
      f(keys)
    }
    catch {
      case e:MissingObjectException => {
        log.debug("Attempt to perform an operation with a non-existent key:" + e.objName)
        throw new WebApplicationException(404)
      }
    }
  }

  def maybeReturn[T](t:T, f: T => Unit) = {
    try {
      f(t)
    }
    catch {
      case e:MissingObjectException => {
        log.debug("Attempt to perform an operation with a non-existent entity: " + t)
        throw new WebApplicationException(404)
      }
    }
    t
  }

  def create[T] (t:T, f: T => Unit, id: T => Any) = {
    try {
      f(t)
      val uri = uriInfo.getAbsolutePathBuilder().path(id(t) + "").build()
      Response.created(uri).build()
    }
    catch {
      case e:Exception => {
        log.error("Could not execute function:" + t, e)
        throw new WebApplicationException(500)
      }
    }
  }
}