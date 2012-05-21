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

package net.lshift.diffa.kernel.config.system

import org.junit.Test
import org.junit.Assert._
import org.easymock.EasyMock._
import org.hibernate.SessionFactory
import net.lshift.diffa.kernel.util.db.DatabaseFacade
import net.lshift.diffa.kernel.config.{User, PairCache}
import org.easymock.classextension.{EasyMock => EasyMock4Classes}
import org.easymock.EasyMock


class CachedSystemConfigStoreTest {


  @Deprecated val sessionFactory = createStrictMock(classOf[SessionFactory])
  val pairCache = EasyMock4Classes.createStrictMock(classOf[PairCache])
  val db = createStrictMock(classOf[DatabaseFacade])

  val systemConfigStore = new HibernateSystemConfigStore(sessionFactory, db, pairCache)

  @Test
  def shouldCacheLookupByToken {
    val user = new User(name = "username", email = "", superuser = false, passwordEnc = "", token = "6f4g4b3c")

    expect(db.singleQueryMaybe[User]("userByName", Map("name" -> user.name))).andReturn(None).once()
    expect(db.execute(EasyMock.eq("insertNewUser"), EasyMock.anyObject[Map[String,Any]])).andReturn(1).once()
    expect(db.singleQuery[User](
      EasyMock.eq("userByToken"),
      EasyMock.eq(Map("token" -> user.token)),
      EasyMock.anyObject[String])).andReturn(user).once()
    replay(db)

    systemConfigStore.createOrUpdateUser(user)
    val retrieved1 = systemConfigStore.getUserByToken("6f4g4b3c")
    assertEquals(user, retrieved1)
    //val retrieved2 = systemConfigStore.getUserByToken("6f4g4b3c")
    //assertEquals(user, retrieved2)

    verify(db)
  }
}
