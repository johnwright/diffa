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
package net.lshift.diffa.kernel.config

import org.jooq.impl.Factory
import net.lshift.diffa.schema.tables.UniqueCategoryNames.UNIQUE_CATEGORY_NAMES
import net.lshift.diffa.schema.tables.PrefixCategories.PREFIX_CATEGORIES
import net.lshift.diffa.schema.tables.SetCategories.SET_CATEGORIES
import net.lshift.diffa.schema.tables.RangeCategories.RANGE_CATEGORIES
import scala.collection.JavaConversions._

/**
 * This object is a workaround for the fact that Scala is so slow
 */
object JooqDomainConfigStoreCompanion {

  val ENDPOINT_TARGET_TYPE = "endpoint"
  val ENDPOINT_VIEW_TARGET_TYPE = "endpoint_view"

  def insertCategories(t:Factory,
                       domain:String,
                       endpoint:String,
                       categories:java.util.Map[String,CategoryDescriptor],
                       viewName: Option[String] = None) = {

    categories.foreach { case (categoryName, descriptor) => {

      val base = t.insertInto(UNIQUE_CATEGORY_NAMES).
                   set(UNIQUE_CATEGORY_NAMES.DOMAIN, domain).
                   set(UNIQUE_CATEGORY_NAMES.ENDPOINT, endpoint).
                   set(UNIQUE_CATEGORY_NAMES.NAME, categoryName)

      val insert = viewName match {

        case Some(view) =>
          base.set(UNIQUE_CATEGORY_NAMES.TARGET_TYPE, ENDPOINT_VIEW_TARGET_TYPE).
            set(UNIQUE_CATEGORY_NAMES.VIEW_NAME, view)
        case None       =>
          base.set(UNIQUE_CATEGORY_NAMES.TARGET_TYPE, ENDPOINT_TARGET_TYPE)

      }

      try {

        insert.execute()

        descriptor match {
          case r:RangeCategoryDescriptor  => insertRangeCategories(t, domain, endpoint, categoryName, r, viewName)
          case s:SetCategoryDescriptor    => insertSetCategories(t, domain, endpoint, categoryName, s, viewName)
          case p:PrefixCategoryDescriptor => insertPrefixCategories(t, domain, endpoint, categoryName, p, viewName)
        }
      }
      catch
        {
          // TODO Catch the unique constraint exception
          case x => throw x
        }
    }}
  }

  def insertPrefixCategories(t:Factory,
                             domain:String,
                             endpoint:String,
                             categoryName:String,
                             descriptor:PrefixCategoryDescriptor,
                             viewName: Option[String] = None) = {

    val insertBase = t.insertInto(PREFIX_CATEGORIES).
                       set(PREFIX_CATEGORIES.DOMAIN, domain).
                       set(PREFIX_CATEGORIES.ENDPOINT, endpoint).
                       set(PREFIX_CATEGORIES.NAME, categoryName).
                       set(PREFIX_CATEGORIES.STEP, Integer.valueOf(descriptor.step)).
                       set(PREFIX_CATEGORIES.MAX_LENGTH, Integer.valueOf(descriptor.maxLength)).
                       set(PREFIX_CATEGORIES.PREFIX_LENGTH, Integer.valueOf(descriptor.prefixLength))

    val insert = viewName match {

      case Some(view) =>
        insertBase.set(PREFIX_CATEGORIES.TARGET_TYPE, ENDPOINT_VIEW_TARGET_TYPE).
          set(PREFIX_CATEGORIES.VIEW_NAME, view)
      case None       =>
        insertBase.set(PREFIX_CATEGORIES.TARGET_TYPE, ENDPOINT_TARGET_TYPE)

    }

    insert.execute()
  }

  def insertSetCategories(t:Factory,
                          domain:String,
                          endpoint:String,
                          categoryName:String,
                          descriptor:SetCategoryDescriptor,
                          viewName: Option[String] = None) = {

    // TODO Is there a way to re-use the insert statement with a bind parameter?

    descriptor.values.foreach(value => {

      val insertBase = t.insertInto(SET_CATEGORIES).
                         set(SET_CATEGORIES.DOMAIN, domain).
                         set(SET_CATEGORIES.ENDPOINT, endpoint).
                         set(SET_CATEGORIES.NAME, categoryName).
                         set(SET_CATEGORIES.VALUE, value)

      val insert = viewName match {

        case Some(view) =>
          insertBase.set(SET_CATEGORIES.TARGET_TYPE, ENDPOINT_VIEW_TARGET_TYPE).
            set(SET_CATEGORIES.VIEW_NAME, view)
        case None       =>
          insertBase.set(SET_CATEGORIES.TARGET_TYPE, ENDPOINT_TARGET_TYPE)

      }

      insert.execute()

    })

  }

  def insertRangeCategories(t:Factory,
                                    domain:String,
                                    endpoint:String,
                                    categoryName:String,
                                    descriptor:RangeCategoryDescriptor,
                                    viewName: Option[String] = None) = {
    val insertBase = t.insertInto(RANGE_CATEGORIES).
      set(RANGE_CATEGORIES.DOMAIN, domain).
      set(RANGE_CATEGORIES.ENDPOINT, endpoint).
      set(RANGE_CATEGORIES.NAME, categoryName).
      set(RANGE_CATEGORIES.DATA_TYPE, descriptor.dataType).
      set(RANGE_CATEGORIES.LOWER_BOUND, descriptor.lower).
      set(RANGE_CATEGORIES.UPPER_BOUND, descriptor.upper).
      set(RANGE_CATEGORIES.MAX_GRANULARITY, descriptor.maxGranularity)

    val insert = viewName match {

      case Some(view) =>
        insertBase.set(RANGE_CATEGORIES.TARGET_TYPE, ENDPOINT_VIEW_TARGET_TYPE).
          set(RANGE_CATEGORIES.VIEW_NAME, view)
      case None       =>
        insertBase.set(RANGE_CATEGORIES.TARGET_TYPE, ENDPOINT_TARGET_TYPE)

    }

    insert.execute()
  }

  def deleteRangeCategories(t:Factory, domain:String, endpoint:String) = {
    t.delete(RANGE_CATEGORIES).
      where(RANGE_CATEGORIES.DOMAIN.equal(domain)).
      and(RANGE_CATEGORIES.ENDPOINT.equal(domain)).
      execute()
  }

  def deleteSetCategories(t:Factory, domain:String, endpoint:String) = {
    t.delete(SET_CATEGORIES).
      where(SET_CATEGORIES.DOMAIN.equal(domain)).
      and(SET_CATEGORIES.ENDPOINT.equal(domain)).
      execute()
  }

  def deletePrefixCategories(t:Factory, domain:String, endpoint:String) = {
    t.delete(PREFIX_CATEGORIES).
      where(PREFIX_CATEGORIES.DOMAIN.equal(domain)).
      and(PREFIX_CATEGORIES.ENDPOINT.equal(domain)).
      execute()
  }

  def deleteCategories(t:Factory, domain:String, endpoint:String) = {
    deletePrefixCategories(t, domain, endpoint)
    deleteSetCategories(t, domain, endpoint)
    deleteRangeCategories(t, domain, endpoint)

    t.delete(UNIQUE_CATEGORY_NAMES).
      where(UNIQUE_CATEGORY_NAMES.DOMAIN.equal(domain)).
      and(UNIQUE_CATEGORY_NAMES.ENDPOINT.equal(domain)).
      execute()
  }

}
