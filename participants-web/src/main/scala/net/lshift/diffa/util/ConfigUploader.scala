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
package net.lshift.diffa.util

import org.apache.http.{HttpHost, HttpEntity}
import org.apache.http.auth.{UsernamePasswordCredentials, AuthScope}
import org.apache.http.impl.client.{BasicAuthCache, DefaultHttpClient}
import org.apache.http.impl.auth.BasicScheme
import org.apache.http.protocol.BasicHttpContext
import org.apache.http.client.protocol.ClientContext
import java.net.URL
import org.apache.http.client.methods.HttpPost

/**
 * Utility for uploading XML configurations to Diffa.
 */
class ConfigUploader(url:String, configEntity:HttpEntity, username:String, password:String) {
  val parsedUrl = new URL(url)

  val targetHost = new HttpHost(parsedUrl.getHost, parsedUrl.getPort)
  val httpClient = new DefaultHttpClient()
  httpClient.getCredentialsProvider.setCredentials(
    new AuthScope(targetHost.getHostName, targetHost.getPort),
    new UsernamePasswordCredentials(username, password))

  val authCache = new BasicAuthCache()
  authCache.put(targetHost, new BasicScheme)
  val localContext = new BasicHttpContext()
  localContext.setAttribute(ClientContext.AUTH_CACHE, authCache);

  val httpPost = new HttpPost(url)
  httpPost.setEntity(configEntity)
  httpClient.execute(httpPost, localContext)
}