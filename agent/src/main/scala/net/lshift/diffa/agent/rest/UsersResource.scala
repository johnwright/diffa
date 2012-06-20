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
package net.lshift.diffa.agent.rest

import javax.ws.rs.{PathParam, Produces, GET, Path}
import org.springframework.stereotype.Component
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.beans.factory.annotation.Autowired
import net.lshift.diffa.kernel.config.system.CachedSystemConfigStore
import com.sun.jersey.api.NotFoundException
import net.lshift.diffa.kernel.preferences.{UserPreferencesStore, FilteredItemType}
import scala.collection.JavaConversions._

@Path("/users/{user}/{domain}")
@Component
@PreAuthorize("hasPermission(#user, 'user-preferences') and hasPermission(#domain, 'domain-user')")
class UsersResource {

  @Autowired var systemConfigStore:CachedSystemConfigStore = null
  @Autowired var userPreferences:UserPreferencesStore = null

  @GET
  @Path("/filter/{itemType}")
  @Produces(Array("application/json"))
  def getFilters(@PathParam("user") user:String,
                 @PathParam("domain") domain:String,
                 @PathParam("itemType") itemType:String) = {
    checkDomain(domain)
    val filterType = getFilterType(itemType)
    userPreferences.listFilteredItems(domain, user, filterType).toArray
  }

  private def getFilterType(unparsed:String) = {
    try {
      FilteredItemType.valueOf(unparsed)
    }
    catch {
      case x:IllegalArgumentException =>
        throw new InvalidEnumException("FilteredItemType", unparsed)
    }
  }

  private def checkDomain[T](domain: String) =
    if (!systemConfigStore.doesDomainExist(domain)) {
      throw new NotFoundException("Invalid domain: " + domain)
    }


}
