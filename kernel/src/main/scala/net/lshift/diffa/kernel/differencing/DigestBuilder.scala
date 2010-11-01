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

import org.joda.time.DateTime
import java.security.MessageDigest
import collection.mutable.{ListBuffer, HashMap}
import net.lshift.diffa.kernel.participants._
import net.lshift.diffa.kernel.events.VersionID
import org.apache.commons.codec.binary.Hex
import scala.collection.Map

/**
 * Utility class for building version digests from a sequence of versions.
 */
class DigestBuilder(val categoryFunction:CategoryFunction) {
  private val digestBuckets = new HashMap[String, Bucket]
  private val versions = new ListBuffer[VersionDigest]

  /**
   * Adds a new version into the builder.
   */
  def add(id:VersionID, attributes:Map[String,String], lastUpdated:DateTime, vsn:String) : Unit 
    = add(id.id, attributes, lastUpdated, vsn)

  def add(id:String, attributes:Map[String,String], lastUpdated:DateTime, vsn:String) {
    val label = attributes.keySet.reduceLeft(_ + "_" + _)
    val categoryNames = attributes.keySet.toSeq
    if (!categoryFunction.shouldBucket) {      
      versions += VersionDigest(categoryNames, lastUpdated, vsn)
    } else {
      val bucket = digestBuckets.get(label) match {
        case None => {
          val newBucket = new Bucket(categoryNames)
          digestBuckets(label) = newBucket
          newBucket
        }
        case Some(b) => b
      }
      bucket.add(vsn)
    }
  }

  /**
   * Retrieves the bucketed digests for all version objects that have been provided.
   */
  def digests:Seq[VersionDigest] = {
    if (categoryFunction.shouldBucket) {
      versions
    } else {
      // Digest the buckets
      digestBuckets.values.map(b => b.toDigest).toList
    }
  }

  private class Bucket(val categoryNames:Seq[String]) {
    private val digestAlgorithm = "MD5"
    private val theDigest =  MessageDigest.getInstance(digestAlgorithm)

    def add(vsn:String) = {
      val vsnBytes = vsn.getBytes("UTF-8")
      theDigest.update(vsnBytes, 0, vsnBytes.length)
    }

    def toDigest = VersionDigest(categoryNames, null, new String(Hex.encodeHex(theDigest.digest())))
  }
}