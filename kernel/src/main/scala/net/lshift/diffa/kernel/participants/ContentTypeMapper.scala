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
 * Maps data from an arbitrary incoming content type to Diffa's default content type.
 */
trait ContentTypeMapper {

  val contentType: String

  def map(content: String): String
}

/**
 * Default (no-op) content type mapper implementation.
 */
case class NullContentTypeMapper(contentType: String) extends ContentTypeMapper {

  def map(content: String) = content
}