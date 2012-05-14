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
package net.lshift.diffa.kernel.util

trait CacheProvider {

  def getCachedMap[V](name:String) : CachedMap[V]

}

case class CacheConfig(ttl:Int)

trait CachedMap[V] {

  def get(key:String) : V
  def put(key:String, value:V)
  def evict(key:String)
  def evictAll

  /**
   * Potentially very expensive operation, use with caution
   */
  def evictByPrefix(prefix:String)

  def readThrough(key:String, f:() => V ) : V = {
    val cached = get(key)
    if (null == cached) {
      val value = f()
      put(key,value)
      value
    }
    else {
      cached
    }
  }
}
