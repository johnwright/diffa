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

/**
 * Utility for helping with validation Diffa configs.
 */
object ValidationUtil {
  /**
   * Build a path out of a parent and child component pair. The parent may be null, in which case only the child
   * component will be used.
   */
  def buildPath(parent:String, child:String):String = if (parent == null) {
      child
    } else {
     parent + "/" + child
    }

  /**
   * Build a path out of a parent and child component pair. The child attributes are used to distinguish a given child
   * that may have multiple instances (for example, a pair). The parent may be null, in which case only the child
   * component will be used.
   */
  def buildPath(parent:String, child:String, childAttrs:Map[String, String]):String = {
    val childAttrStr = childAttrs.map { case (k, v) => k + "=" + v }.reduceLeft(_ + ", " + _)
    buildPath(parent, child + "[" + childAttrStr + "]")
  }

}