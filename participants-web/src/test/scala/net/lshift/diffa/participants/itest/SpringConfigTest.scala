/**
 * Copyright (C) 2011 LShift Ltd.
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

package net.lshift.diffa.participants.itest

import org.junit.Test
import org.junit.Assert._
import java.net.{HttpURLConnection, URL}

class SpringConfigTest {

  @Test
  def testHttpResponseOK {
    val url = new URL("http://localhost:19293/participant-demo/")
    val responseCode = url.openConnection match {
      case conn: HttpURLConnection => conn.getResponseCode()
    }
    assertEquals("Participants app did not respond OK", 200, responseCode)
  }

}
