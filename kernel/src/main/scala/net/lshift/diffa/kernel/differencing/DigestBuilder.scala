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
import net.lshift.diffa.kernel.util.DateUtils._

/**
 * Utility class for building version digests from a sequence of versions.
 */
class DigestBuilder(val gran:RangeGranularity) {
  private val digestBuckets = new HashMap[String, Bucket]
  private val versions = new ListBuffer[VersionDigest]

  /**
   * Adds a new version into the builder.
   */
  def add(id:VersionID, date:DateTime, lastUpdated:DateTime, vsn:String):Unit = add(id.id, date, lastUpdated, vsn)
  def add(id:String, date:DateTime, lastUpdated:DateTime, vsn:String) {
      if (!isBucketing) {
      versions += VersionDigest(id, date, lastUpdated, vsn)
    } else {
      val bucketName = buildBucketName(date)
      val bucket = digestBuckets.get(bucketName) match {
        case None => {
          val newBucket = new Bucket(bucketName, normaliseDate(date))
          digestBuckets(bucketName) = newBucket
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
    if (!isBucketing) {
      versions
    } else {
      // Digest the buckets
      digestBuckets.values.map(b => b.toDigest).toList
    }
  }

  /**
   * Don't bucket for individual versions.
   */
  private def isBucketing = gran != IndividualGranularity

  /**
   * Generates a name for a bucket based on a date.
   */
  private def buildBucketName(date:DateTime) = gran match {
    case DayGranularity   => date.toString("yyyy-MM-dd")
    case MonthGranularity => date.toString("yyyy-MM")
    case YearGranularity  => date.toString("yyyy")
  }
  private def normaliseDate(date:DateTime) = gran match {
    case DayGranularity   => startOfDay(date)
    case MonthGranularity => startOfDay(date).withDayOfMonth(1)
    case YearGranularity => startOfDay(date).withDayOfYear(1)
  }

  private class Bucket(val name:String, val date:DateTime) {
    private val digestAlgorithm = "MD5"
    private val digest =  MessageDigest.getInstance(digestAlgorithm)

    def add(vsn:String) = {
      val vsnBytes = vsn.getBytes("UTF-8")
      digest.update(vsnBytes, 0, vsnBytes.length)
    }

    def toDigest = VersionDigest(name, date, null, new String(Hex.encodeHex(digest.digest)))
  }
}