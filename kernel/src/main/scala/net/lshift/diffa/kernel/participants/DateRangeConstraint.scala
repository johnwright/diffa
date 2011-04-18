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

import org.joda.time.LocalDate
import net.lshift.diffa.kernel.config.DateTypeDescriptor

/**
 * A constraint for a Year-Month-Day-typed field that should have a <code>cat</code> field with its value between:
 *
 * <li>
 *   <ul>yyyy-MM-ddT00:00:00.000Z</ul>
 *   <ul>yyyy-MM-ddT23:59:59.999Z</ul>
 * </li>
 *
 * inclusively.
 */
case class DateRangeConstraint(cat:String, start:LocalDate, end:LocalDate)
  extends RangeQueryConstraint(cat, start.toString(), end.toString()) {

  override def dataType = DateTypeDescriptor
}