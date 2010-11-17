/**
 * Copyright (C) 2010 LShift Ltd.
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
import scala.collection.Map

/**
 * Utility methods for differencing sequences of digests.
 */
object DigestDifferencingUtils {

  def differenceEntities(ds1:Seq[EntityVersion],
                         ds2:Seq[EntityVersion],
                         resolve:Digest => Map[String,String],
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
        result += VersionMismatch(label, resolve(ds1Digest), ds1Digest.lastUpdated, ds1Digest.digest, otherDigest)
      }
    }}

    ds2Ids.foreach { case (label, hs2Digest) => {
      val otherMatches = ds1Ids.remove(label) match {
        case None => false
        case _    => true    // No need to compare, since we did that above
      }

      if (!otherMatches) {
        result += VersionMismatch(label, resolve(hs2Digest), hs2Digest.lastUpdated, null, hs2Digest.digest)
      }
    }}

    result
  }

  def differenceAggregates(ds1:Seq[AggregateDigest],
                           ds2:Seq[AggregateDigest],
                           resolve:Digest => Map[String,String],
                           constraints:Seq[QueryConstraint]) : Seq[QueryAction] = {
    assert(constraints.length < 2, "See ticket #148")
    var preemptVersionQuery = false
    val results = new ListBuffer[QueryAction]
    val ds1Ids = indexByAttributeValues(ds1)
    val ds2Ids = indexByAttributeValues(ds2)

    def evaluate(partition:String) = {
        val empty = ds1.isEmpty || ds2.isEmpty
        constraints(0).nextQueryAction(partition, empty) match {
          case None    => preemptVersionQuery = true
          case Some(x) => results += x
        }
    }

    ds1Ids.foreach { case (label, ds1Digest) => {
      val (otherMatches, otherDigest, otherDigestUpdated) = ds2Ids.remove(label) match {
        case Some(hs2Digest) => (ds1Digest.digest == hs2Digest.digest, hs2Digest.digest, hs2Digest.lastUpdated)
        case None => (false, null, null)
      }

      if (!otherMatches) {
        assert(ds1Digest.attributes.length < 2, "See ticket #148")
        val partition = ds1Digest.attributes(0)
        evaluate(partition)
      }
    }}

    ds2Ids.foreach { case (label, hs2Digest) => {
      val otherMatches = ds1Ids.remove(label) match {
        case None => false
        case _    => true    // No need to compare, since we did that above
      }

      if (!otherMatches) {
        assert(hs2Digest.attributes.length < 2, "See ticket #148")
        val partition = hs2Digest.attributes(0)
        evaluate(partition)
      }
    }}

    if (preemptVersionQuery) {
      // TODO [#2] does not expand to multiple constraints yet
      List(EntityQueryAction(constraints(0)))  
    }
    else {
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
