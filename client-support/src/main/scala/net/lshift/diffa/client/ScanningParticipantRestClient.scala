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

package net.lshift.diffa.client

import net.lshift.diffa.participant.scanning.{ScanResultEntry, ScanConstraint}
import net.lshift.diffa.kernel.util.AlertCodes._
import org.apache.http.params.{HttpConnectionParams, BasicHttpParams}
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.HttpClient
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import net.lshift.diffa.participant.common.JSONHelper
import org.apache.http.util.EntityUtils
import com.sun.jersey.core.util.MultivaluedMapImpl
import net.lshift.diffa.kernel.participants.{ScanningParticipantRef, CategoryFunction}
import org.apache.http.message.BasicNameValuePair
import org.apache.http.client.utils.URLEncodedUtils
import org.apache.http.auth.{UsernamePasswordCredentials, AuthScope}
import java.net.URI


/**
 * A ScanningParticipantRestClient is responsible for issuing scan queries to
 * Participants and mapping the JSON response to an Object.
 */
class ScanningParticipantRestClient(scanUrl: String, params: RestClientParams = RestClientParams.default)
  extends ScanningParticipantRef {

  implicit private val logger = LoggerFactory.getLogger(getClass)
  private final val ACCEPTABLE_STATUS_CODE = 200
  private val uri = new URI(scanUrl)

  def scan(constraints: Seq[ScanConstraint], aggregations: Seq[CategoryFunction]): Seq[ScanResultEntry] = {
    val httpClient = createHttpClient(new BasicHttpParams)
    httpClient.getCredentialsProvider.setCredentials(
      new AuthScope(uri.getHost, uri.getPort),
      new UsernamePasswordCredentials(params.username.get, params.password.get)
    )
    val httpGet = setQueryParams(constraints, aggregations)

    // TODO: set Content-Type: application/JSON
    val response = try {
      httpClient.execute(httpGet)
      // Note that ScanFailedException is no longer thrown on connect failure.
      // Instead, any exception thrown by execute is propagated up the stack.
    } finally {
      shutdownImmediate(httpClient)
    }

    val statusCode = response.getStatusLine.getStatusCode
    statusCode match {
      case ACCEPTABLE_STATUS_CODE => JSONHelper.readQueryResult(response.getEntity.getContent)
      case _ =>
        logger.error("{} External scan error, response code: {}",
          Array(formatAlertCode(EXTERNAL_SCAN_ERROR), statusCode))
        throw new Exception("Participant scan failed: %s\n%s".format(
          statusCode, EntityUtils.toString(response.getEntity)))
    }
  }

  def setQueryParams(constraints: Seq[ScanConstraint], aggregations: Seq[CategoryFunction]) = {
    val qryParams = new MultivaluedMapImpl
    RequestBuildingHelper.constraintsToQueryArguments(qryParams, constraints)
    RequestBuildingHelper.aggregationsToQueryArguments(qryParams, aggregations)

    val qParams = {
      for (p <- qryParams.entrySet; v <- p.getValue)
      yield new BasicNameValuePair(p.getKey, v)
    }.toSeq

    val paramString = URLEncodedUtils.format(qParams, "UTF-8")
    val httpGet = new HttpGet("%s?%s".format(scanUrl, paramString))
    httpGet
  }

  private def createHttpClient(httpParams: BasicHttpParams): DefaultHttpClient = {
    params.connectTimeout foreach { HttpConnectionParams.setConnectionTimeout(httpParams, _) }
    params.readTimeout foreach { HttpConnectionParams.setSoTimeout(httpParams, _) }
    new DefaultHttpClient(httpParams)
  }

  private def shutdownImmediate(client: HttpClient) {
    try {
      client.getConnectionManager.shutdown
    } catch {
      case e =>
        logger.warn("Could not shut down HTTP client: {} {}",
          Array[Object](formatAlertCode(ACTION_HTTP_CLEANUP_FAILURE), e.getClass, e.getMessage))
    }
  }
}
