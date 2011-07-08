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

package net.lshift.diffa.kernel.differencing

import net.lshift.diffa.kernel.events._
import net.lshift.diffa.kernel.participants._
import org.joda.time.DateTime
import net.lshift.diffa.kernel.alerting.Alerter
import scala.collection.JavaConversions._
import net.lshift.diffa.kernel.config.{Endpoint, Pair, ConfigStore}
import org.slf4j.LoggerFactory
import concurrent.SyncVar
import scala.collection.JavaConversions._
import net.lshift.diffa.participant.scanning.{ScanConstraint, DigestBuilder, ScanResultEntry}
import net.lshift.diffa.kernel.diag.{TraceLevel, DiagnosticsManager}

/**
 * Standard behaviours supported by synchronising version policies.
 */
abstract class BaseSynchingVersionPolicy(val stores:VersionCorrelationStoreFactory,
                                         listener:DifferencingListener,
                                         configStore:ConfigStore,
                                         diagnostics:DiagnosticsManager)
    extends VersionPolicy {
  protected val alerter = Alerter.forClass(getClass)

  /**
   * Handles a participant change. Due to the need to later correlate data, event information is cached to the
   * version correlation store.
   */
  def onChange(writer: LimitedVersionCorrelationWriter, evt: PairChangeEvent) = {

    val pair = configStore.getPair(evt.id.pairKey)

    val corr = evt match {
      case UpstreamPairChangeEvent(id, _, lastUpdate, vsn) => vsn match {
        case null => writer.clearUpstreamVersion(id)
        case _    => writer.storeUpstreamVersion(id, pair.upstream.schematize(evt.attributes), maybe(lastUpdate), vsn)
      }
      case DownstreamPairChangeEvent(id, _, lastUpdate, vsn) => vsn match {
        case null => writer.clearDownstreamVersion(id)
        case _    => writer.storeDownstreamVersion(id, pair.downstream.schematize(evt.attributes), maybe(lastUpdate), vsn, vsn)
      }
      case DownstreamCorrelatedPairChangeEvent(id, _, lastUpdate, uvsn, dvsn) => (uvsn, dvsn) match {
        case (null, null) => writer.clearDownstreamVersion(id)
        case _            => writer.storeDownstreamVersion(id, pair.downstream.schematize(evt.attributes), maybe(lastUpdate), uvsn, dvsn)
      }
    }

    if (corr.isMatched.booleanValue) {
      listener.onMatch(evt.id, corr.upstreamVsn, LiveWindow)
    } else {
      listener.onMismatch(evt.id, corr.lastUpdate, corr.upstreamVsn, corr.downstreamUVsn, LiveWindow)
    }
  }

  def maybe(lastUpdate:DateTime) = {
    lastUpdate match {
      case null => new DateTime
      case d    => d
    }
  }

  def replayUnmatchedDifferences(pairKey:String, l:DifferencingListener, origin:MatchOrigin) {
    val pair = configStore.getPair(pairKey)
    generateDifferenceEvents(pair, l, origin)
  }

  def scanUpstream(pairKey:String, writer: LimitedVersionCorrelationWriter, participant:UpstreamParticipant,
                   listener:DifferencingListener, handle:FeedbackHandle) = {
    val pair = configStore.getPair(pairKey)
    val upstreamConstraints = pair.upstream.groupedConstraints
    constraintsOrEmpty(upstreamConstraints).foreach((new UpstreamSyncStrategy)
      .scanParticipant(pair, writer, pair.upstream, pair.upstream.defaultBucketing, _, participant, listener, handle))
  }

  def scanDownstream(pairKey:String, writer: LimitedVersionCorrelationWriter, us:UpstreamParticipant,
                     ds:DownstreamParticipant, listener:DifferencingListener, handle:FeedbackHandle) = {
    val pair = configStore.getPair(pairKey)
    val downstreamConstraints = pair.downstream.groupedConstraints
    constraintsOrEmpty(downstreamConstraints).foreach(downstreamStrategy(us,ds)
      .scanParticipant(pair, writer, pair.downstream, pair.downstream.defaultBucketing, _, ds, listener, handle))
  }

  private def constraintsOrEmpty(grouped:Seq[Seq[ScanConstraint]]):Seq[Seq[ScanConstraint]] =
    if (grouped.length > 0)
      grouped
    else
      Seq(Seq())

  private def generateDifferenceEvents(pair:Pair, l:DifferencingListener, origin:MatchOrigin) {
    // Run a query for mismatched versions, and report each one
    stores(pair.key).unmatchedVersions(pair.upstream.defaultConstraints, pair.downstream.defaultConstraints).foreach(
      corr => l.onMismatch(VersionID(corr.pairing, corr.id), corr.lastUpdate, corr.upstreamVsn, corr.downstreamUVsn, origin))
  }

  /**
   * Allows an implementing policy to define what kind of downstream syncing policy it requires
   */
  def downstreamStrategy(us:UpstreamParticipant, ds:DownstreamParticipant) : SyncStrategy

  /**
   * The basic functionality for a synchronisation strategy.
   */
  protected abstract class SyncStrategy {

    val log = LoggerFactory.getLogger(getClass)

    def scanParticipant(pair:Pair,
                        writer:LimitedVersionCorrelationWriter,
                        endpoint:Endpoint,
                        bucketing:Seq[CategoryFunction],
                        constraints:Seq[ScanConstraint],
                        participant:Participant,
                        listener:DifferencingListener,
                        handle:FeedbackHandle) {

      checkForCancellation(handle, pair)
      diagnostics.logPairEvent(TraceLevel, pair.key, "Scanning aggregates for %s with (constraints=%s, bucketing=%s)".format(endpoint.name, constraints, bucketing))

      val remoteDigests = participant.scan(constraints, bucketing)
      val localDigests = getAggregates(pair.key, bucketing, constraints)

      if (log.isTraceEnabled) {
        log.trace("Bucketing: %s".format(bucketing))
        log.trace("Constraints: %s".format(constraints))
        log.trace("Remote digests: %s".format(remoteDigests))
        log.trace("Local digests: %s".format(localDigests))
      }

      DigestDifferencingUtils.differenceAggregates(remoteDigests, localDigests, bucketing, constraints).foreach(o => o match {
        case AggregateQueryAction(narrowBuckets, narrowConstraints) =>
          scanParticipant(pair, writer, endpoint, narrowBuckets, narrowConstraints, participant, listener, handle)
        case EntityQueryAction(narrowed)    => {

          checkForCancellation(handle, pair)
          diagnostics.logPairEvent(TraceLevel, pair.key, "Scanning entities for {0} with (constraints={1})".format(endpoint.name, narrowed))

          val remoteVersions = participant.scan(narrowed, Seq())
          val cachedVersions = getEntities(pair.key, narrowed)

          if (log.isTraceEnabled) {
            log.trace("Remote versions: %s".format(remoteVersions))
            log.trace("Local versions: %s".format(cachedVersions))
          }
          
          DigestDifferencingUtils.differenceEntities(endpoint.categories.toMap, remoteVersions, cachedVersions, narrowed)
            .foreach(handleMismatch(pair.key, writer, _, listener))
        }
      })
    }

    def checkForCancellation(handle:FeedbackHandle, pair:Pair) = {
      if (handle.isCancelled) {
        throw new ScanCancelledException(pair.key)
      }
    }

    /**
     * Should be invoked by the child sync strategies each time they modify a correlation (eg, store an upstream or
     * downstream version). This allows for any necessary eventing to be performed.
     */
    protected def handleUpdatedCorrelation(corr:Correlation) {
      // Unmatched versions will be evented at the end of the sync. Matched versions should be evented immediately, as
      // we won't know what went from unmatched -> matched later.
      if (corr.isMatched.booleanValue) {
        listener.onMatch(VersionID(corr.pairing, corr.id), corr.upstreamVsn, TriggeredByScan)
      }
    }

    def getAggregates(pairKey:String, bucketing:Seq[CategoryFunction], constraints:Seq[ScanConstraint]) : Seq[ScanResultEntry]
    def getEntities(pairKey:String, constraints:Seq[ScanConstraint]) : Seq[ScanResultEntry]
    def handleMismatch(pairKey:String, writer: LimitedVersionCorrelationWriter, vm:VersionMismatch, listener:DifferencingListener)
  }

  protected class UpstreamSyncStrategy extends SyncStrategy {

    def getAggregates(pairKey:String, bucketing:Seq[CategoryFunction], constraints:Seq[ScanConstraint]) = {
      val aggregator = new Aggregator(bucketing)
      stores(pairKey).queryUpstreams(constraints, aggregator.collectUpstream)
      aggregator.digests
    }

    def getEntities(pairKey:String, constraints:Seq[ScanConstraint]) = {
      stores(pairKey).queryUpstreams(constraints).map(x => {
        ScanResultEntry.forEntity(x.id, x.upstreamVsn, x.lastUpdate, mapAsJavaMap(x.upstreamAttributes))
      })
    }

    def handleMismatch(pairKey: String, writer: LimitedVersionCorrelationWriter, vm:VersionMismatch, listener:DifferencingListener) = {
      vm match {
        case VersionMismatch(id, attributes, lastUpdate,  usVsn, _) =>
          if (usVsn != null) {
            handleUpdatedCorrelation(writer.storeUpstreamVersion(VersionID(pairKey, id), attributes, lastUpdate, usVsn))
          } else {
            handleUpdatedCorrelation(writer.clearUpstreamVersion(VersionID(pairKey, id)))
          }
      }
    }
  }

  protected class Aggregator(bucketing:Seq[CategoryFunction]) {
    val builder = new DigestBuilder(bucketing)

    def collectUpstream(id:VersionID, attributes:Map[String, String], lastUpdate:DateTime, vsn:String) =
      builder.add(id.id, attributes, vsn)
    def collectDownstream(id:VersionID, attributes:Map[String, String], lastUpdate:DateTime, uvsn:String, dvsn:String) =
      builder.add(id.id, attributes, dvsn)

    def digests:Seq[ScanResultEntry] = builder.toDigests
  }
}