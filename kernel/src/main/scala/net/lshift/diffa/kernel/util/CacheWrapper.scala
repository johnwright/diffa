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

package net.lshift.diffa.kernel.util

import java.io.Closeable
import net.sf.ehcache.{Element, CacheManager}
import scala.collection.JavaConversions._

/**
 * Simple wrapper around an underlying EhCache to make its usage less verbose.
 */
class CacheWrapper[A, B](cacheName:String, manager:CacheManager) extends Closeable {

  if (manager.cacheExists(cacheName)) {
    manager.removeCache(cacheName)
  }

  manager.addCache(cacheName)

  val cache = manager.getEhcache(cacheName)

  /**
   * Notifies the cache manager that this cache can be removed
   */
  def close() = manager.removeCache(cacheName)


  /**
   * Removes all values from this cache
   */
  def clear() = cache.removeAll()

  /**
   * Removes the element stored under this key from the cache
   */
  def contains(key:A) = cache.isKeyInCache(key)

  /**
   * Retrieve a value from the value by its key
   */
  def get(key:A) : Option[B] = {
    val element = cache.get(key)
    if (element != null) {
      Some(element.getValue.asInstanceOf[B])
    }
    else {
      None
    }
  }

  /**
   * Add an value to the cache
   */
  def put(key:A, value:B) = cache.put(new Element(key,value))

  /**
   * Returns a list of keys associated with this cache
   */
  def keys : Seq[A] = cache.getKeys.toSeq.map(_.asInstanceOf[A])

}