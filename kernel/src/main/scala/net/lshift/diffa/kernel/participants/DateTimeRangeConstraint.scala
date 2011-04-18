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

import org.joda.time.{DateTime}

/**
 * A constraint for a datetime-typed field that should have a <code>cat</code> field with its value between start and
 * end (inclusive).
 *
 * This data type can be configured either of the two formats:
 *
 * <li>
 *   <ul>yyyy-MM-ddThh:mm:ss.mmmZ, whereby the range will be inclusive of each bound</ul>
 *   <ul>yyyy-MM-dd, whereby the range will be inclusive of yyyy-MM-ddT00:00:00.000Z and yyyy-MM-ddT23:59:59.999Z</ul>
 * </li>
 *
 * This implies that his data type has millisecond granularity.
 *
 */
case class DateTimeRangeConstraint(cat:String, start:DateTime, end:DateTime)
    extends RangeQueryConstraint(cat, start.toString(), end.toString())