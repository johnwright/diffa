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

package net.lshift.diffa.kernel.util

import org.easymock.EasyMock

/**
 * Created by IntelliJ IDEA.
 * User: paulj
 * Date: 09-Jul-2010
 * Time: 10:28:10
 * To change this template use File | Settings | File Templates.
 */

object EasyMockScalaUtils {
  /**
   * Replacement for EasyMock.anyObject when working with a Function3 that returns Unit.
   */
  def anyUnitF3[A, B, C] = {
    EasyMock.anyObject
    (a:A, b:B, c:C) => {}
  }

  /**
   * Replacement for EasyMock.anyObject when working with a Function4 that returns Unit.
   */
  def anyUnitF4[A, B, C, D] = {
    EasyMock.anyObject
    (a:A, b:B, c:C, d:D) => {}
  }

  /**
   * Replacement for EasyMock.anyObject when working with a Function5 that returns Unit.
   */
  def anyUnitF5[A, B, C, D, E] = {
    EasyMock.anyObject
    (a:A, b:B, c:C, d:D, e:E) => {}
  }

  def anyString = {
    EasyMock.anyObject
    ""
  }
}