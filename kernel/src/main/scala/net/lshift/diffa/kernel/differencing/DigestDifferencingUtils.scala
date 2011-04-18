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

import collection.mutable.{ListBuffer, HashMap}
import net.lshift.diffa.kernel.participants._
import net.lshift.diffa.kernel.config.CategoryDescriptor

/**
 * Utility methods for differencing sequences of digests.
 */
object DigestDifferencingUtils {

  def differenceEntities(categories:Map[String, CategoryDescriptor],
                         ds1:Seq[EntityVersion],
                         ds2:Seq[EntityVersion],
                         constraints:Seq[QueryConstraint]) : Seq[VersionMismatch] = {
    val result = new ListBuffer[VersionMismatch]
    val ds1Ids = indexById(ds1)
    val ds2Ids = indexById(ds2)

    ds1Ids.foreach { case (label, ds1Digest) => {
      val (otherMatches, otherDigest, otherDigestUpdated) = ds2Ids.remove(label) match {
        case Some(hs2Digest) => (ds1Digest.digest == hs2Digest.digest, hs2Digest.digest, hs2Digest.lastUpdated)
        case None => (false, null, null)
      }

      if (!otherMatches) {
        result += VersionMismatch(label, AttributesUtil.toTypedMap(categories, ds1Digest.attributes), ds1Digest.lastUpdated, ds1Digest.digest, otherDigest)
      }
    }}

    ds2Ids.foreach { case (label, hs2Digest) => {
      val otherMatches = ds1Ids.remove(label) match {
        case None => false
        case _    => true    // No need to compare, since we did that above
      }

      if (!otherMatches) {
        result += VersionMismatch(label, AttributesUtil.toTypedMap(categories, hs2Digest.attributes), hs2Digest.lastUpdated, null, hs2Digest.digest)
      }
    }}

    result
  }

  def differenceAggregates(ds1:Seq[AggregateDigest],
                           ds2:Seq[AggregateDigest],
                           bucketing:Map[String, CategoryFunction],
                           constraints:Seq[QueryConstraint]) : Seq[QueryAction] = {
    var preemptVersionQuery = false
    val results = new ListBuffer[QueryAction]
    val ds1Ids = indexByAttributeValues(ds1)
    val ds2Ids = indexByAttributeValues(ds2)

    def evaluate(partitions:Map[String, String]) = try {
        val empty = ds1.isEmpty || ds2.isEmpty

        val nextConstraints = constraints.map(c => {
          // Narrow the constraints for those that we had partitions for. Non-bucketing constraints stay the same.
          if (bucketing.contains(c.category)) {
            val partition = partitions(c.category)

            bucketing(c.category).constrain(c, partition)
          } else {
            c
          }
        })
        val nextBuckets = new HashMap[String, CategoryFunction]
        bucketing.foreach { case (attrName, bucket) => {
          bucket.descend match {
            case None =>    // Do nothing - we won't bucket on this next time
            case Some(IndividualCategoryFunction) => // No more bucketing on this attribute
            case Some(fun) => nextBuckets(attrName) = fun
          }
        }}

        if (nextBuckets.size > 0 && !empty) {
          results += AggregateQueryAction(nextBuckets.toMap, nextConstraints)
        } else {
          results += EntityQueryAction(nextConstraints)
        }
    } catch {
      case e: Exception =>
        throw new RuntimeException(
          "%s { bucketing = %s, constraints = %s, partitions = %s, results = %s }"
            .format(e.getMessage, bucketing, constraints, partitions, results), e)
    }

    ds1Ids.foreach { case (label, ds1Digest) => {
      val (otherMatches, otherDigest) = ds2Ids.remove(label) match {
        case Some(hs2Digest) => (ds1Digest.digest == hs2Digest.digest, hs2Digest.digest)
        case None => (false, null)
      }

      if (!otherMatches) {
        evaluate(AttributesUtil.toMap(bucketing.keys, ds1Digest.attributes))
      }
    }}

    ds2Ids.foreach { case (label, hs2Digest) => {
      val otherMatches = ds1Ids.remove(label) match {
        case None => false
        case _    => true    // No need to compare, since we did that above
      }

      if (!otherMatches) {
        evaluate(AttributesUtil.toMap(bucketing.keys, hs2Digest.attributes))
      }
    }}

    if (preemptVersionQuery) {
      List(EntityQueryAction(constraints))
    } else {
      results
    }
  }

  private def indexById(hs:Seq[EntityVersion]) = {
    val res = new HashMap[String, EntityVersion]
    hs.foreach(d => res(d.id) = d)
    res
  }
  private def indexByAttributeValues(hs:Seq[Digest]) = {
    val res = new HashMap[String, Digest]
    hs.foreach(d => res(d.attributes.reduceLeft(_+_)) = d)
    res
  }

}
