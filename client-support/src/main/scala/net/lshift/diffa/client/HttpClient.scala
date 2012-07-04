package net.lshift.diffa.client

import org.apache.http.client.HttpClient
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.client.methods.HttpGet
import java.net.URI

/**
 * Copyright (C) 2010-2012 LShift Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

class ApacheHttpClient extends DiffaHttpClient {
  lazy val client = new DefaultHttpClient
  override def get(r : DiffaHttpQuery) = {
    val req = new HttpGet(r.fullUri)
    println("Request: %s".format(req.getURI))
    client.execute(req)
    Left(new Exception)
  }

}
