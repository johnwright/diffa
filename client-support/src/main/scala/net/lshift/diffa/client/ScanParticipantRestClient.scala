package net.lshift.diffa.client

import net.lshift.diffa.participant.scanning.{ScanResultEntry, ScanAggregation, ScanConstraint}
import javax.ws.rs.core.MultivaluedMap
import com.sun.jersey.core.util.MultivaluedMapImpl
import java.io.InputStream
import net.lshift.diffa.kernel.config._
import net.lshift.diffa.kernel.participants.{CategoryFunction, ScanningParticipantRef}
import org.slf4j.LoggerFactory
import org.apache.http.client.methods.HttpUriRequest
import org.apache.http.client.HttpClient
import java.net.{SocketTimeoutException, SocketException, ConnectException, URI}
import scala.collection.JavaConversions._
import net.lshift.diffa.kernel.differencing.ScanFailedException
import net.lshift.diffa.kernel.util.AlertCodes._
import net.lshift.diffa.kernel.config.DiffaPairRef
import net.lshift.diffa.kernel.config.QueryParameterCredentials
import net.lshift.diffa.kernel.config.BasicAuthCredentials
import scala.Right
import scala.Some
import net.lshift.diffa.client.DiffaHttpQuery
import scala.Left

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

case class DiffaHttpQuery(uri: String,
                          accept: Option[String] = None,
                          query: Map[String, Seq[String]] = Map(),
                          basicAuth: Option[(String, String)] = None) {
  def accepting(content: String) = copy(accept=Some(content))
  def withQuery(query: Map[String, Seq[String]]) = copy(query=query)
  def withConstraints(constraints: Seq[ScanConstraint] ) = {
    withMultiValuedMapQuery(RequestBuildingHelper.constraintsToQueryArguments(_, constraints))
  }

  def withBasicAuth(user: String, passwd: String) = copy(basicAuth = Some((user, passwd)))

  def withAggregations(aggregations: Seq[ScanAggregation]) =
    withMultiValuedMapQuery(RequestBuildingHelper.aggregationsToQueryArguments(_, aggregations))

  private def withMultiValuedMapQuery(updator: MultivaluedMap[String, String] => Unit) = {
    val mvm = new MultivaluedMapImpl()
    updator(mvm)
    val nquery = mvm.foldLeft(query) {
      case (query, (key, value) ) => query + (key -> (query.getOrElse(key, Seq()) ++ value))
    }
    copy(query = nquery)
  }
}

trait DiffaHttpClient {
  def get(query:DiffaHttpQuery) : Either[Throwable, InputStream]

}

class ScanParticipantRestClient(pair: DiffaPairRef,
                                scanUrl: String,
                                serviceLimitsView: PairServiceLimitsView,
                                credentialsLookup: DomainCredentialsLookup,
                                httpClient: DiffaHttpClient = null)
  extends ScanningParticipantRef {

  private val log = LoggerFactory.getLogger(getClass)
  private val restClient = new InternalRestClient(pair, scanUrl, serviceLimitsView, credentialsLookup) {
    def constructGetRequest(queryParams:MultivaluedMapImpl,
                            credentials:Option[QueryParameterCredentials]) =
      super.buildGetRequest(queryParams, credentials)
    def possiblyAuthenticate(prepareRequest:Option[QueryParameterCredentials] => HttpUriRequest) =
      super.maybeAuthenticate(prepareRequest)

    def shutdown(client: HttpClient) = super.shutdownImmediate(client)

  }

  def nullJsonParser = new JsonScanResultParser {
    def parse(stream: InputStream): Array[ScanResultEntry] = Array[ScanResultEntry]()
  }

  def scan(constraints: Seq[ScanConstraint], aggregations: Seq[CategoryFunction]) =
    scan(constraints, aggregations, nullJsonParser)

  def scan(constraints: Seq[ScanConstraint], aggregations: Seq[CategoryFunction],
           parser: JsonScanResultParser): Seq[ScanResultEntry] = {


    val query = DiffaHttpQuery(scanUrl).accepting("application/json").
      withConstraints(constraints).
      withAggregations(aggregations)

    val credentials = credentialsLookup.credentialsForUri(pair.domain, new URI(scanUrl))

    val queryWithCredentials = credentials match {
      case None => query
      case Some(BasicAuthCredentials(user, password)) => query.withBasicAuth(user, password)
      case Some(QueryParameterCredentials(name, value)) => query.withQuery(Map(name -> Seq(value)))
    }
    this.httpClient.get(queryWithCredentials) match {
      case Right(stream) => parser.parse(stream)
      case Left(ex) => handleHttpError(ex, queryWithCredentials)

    }
  }

  def handleHttpError(ex: Throwable, query: DiffaHttpQuery) = ex match {
    case ex: ConnectException =>
      log.error("%s Connection to %s refused".format(SCAN_CONNECTION_REFUSED, scanUrl))
      // NOTICE: ScanFailedException is handled specially (see its class documentation).
      throw new ScanFailedException("Could not connect to " + scanUrl)
    case ex: SocketException =>
      log.error("%s Socket closed to %s".format(SCAN_CONNECTION_CLOSED, scanUrl))
      // NOTICE: ScanFailedException is handled specially (see its class documentation).
      throw new ScanFailedException("Connection to %s closed unexpectedly, query %s".format(
        scanUrl, query.query))
    case ex: SocketTimeoutException =>
      log.error("%s Socket time out for %s".format(SCAN_SOCKET_TIMEOUT, scanUrl))
      // NOTICE: ScanFailedException is handled specially (see its class documentation).
      throw new ScanFailedException("Socket to %s timed out unexpectedly, query %s".format(
        scanUrl, query.query))
    case ex => throw ex
  }
}


