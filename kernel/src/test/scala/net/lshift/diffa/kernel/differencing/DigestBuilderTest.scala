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

import org.junit.Test
import org.junit.Assert._
import net.lshift.diffa.kernel.events.VersionID
import org.joda.time.DateTime
import org.apache.commons.codec.digest.DigestUtils
import net.lshift.diffa.kernel.util.Dates._
import net.lshift.diffa.kernel.participants._
import collection.mutable.HashMap
import collection.immutable.HashSet

/**
 * Test cases for the digest builder.
 */
class DigestBuilderTest {
  val pair = "A-B"
  val categories = new HashMap[String,String]

  def withBucketing(cf:CategoryFunction) = Map("bizDate" -> cf)

  def add(b:DigestBuilder, v:VersionID, d:DateTime, s:String) = b.add(v, Map("bizDate" -> d.toString()), d, s)

  @Test
  def shouldNotBucketIndividualVersions {
    val builder = new DigestBuilder(withBucketing(IndividualCategoryFunction))

    add(builder, VersionID(pair, "id1"), JUL_9_2010_1, "vsn1")
    add(builder, VersionID(pair, "id2"), JUL_9_2010_1, "vsn2")

    assertEquals(
      HashSet(EntityVersion("id1", Seq(JUL_9_2010_1.toString), JUL_9_2010_1, "vsn1"),
              EntityVersion("id2", Seq(JUL_9_2010_1.toString), JUL_9_2010_1, "vsn2")),
      HashSet(builder.versions: _*))
  }

  @Test
  def shouldBucketByDay {
    val builder = new DigestBuilder(withBucketing(DailyCategoryFunction))

    add(builder, VersionID(pair, "id1"), JUL_8_2010_1, "vsn1")
    add(builder, VersionID(pair, "id2"), JUL_8_2010_2, "vsn2")
    add(builder, VersionID(pair, "id3"), JUL_9_2010_1, "vsn3")

    assertEquals(builder.digests, builder.digests)

    assertEquals(
      HashSet(
        // TODO [#2] Should the youngest or lastUpDated date be taken for the comparison?
        AggregateDigest(Seq("2010-07-08"), JUL_8_2010_1, DigestUtils.md5Hex("vsn1" + "vsn2")),
        AggregateDigest(Seq("2010-07-09"), JUL_9_2010_1, DigestUtils.md5Hex("vsn3"))),
      HashSet(builder.digests: _*))
  }

  @Test
  def shouldBucketByMonth {
    val builder = new DigestBuilder(withBucketing(MonthlyCategoryFunction))

    add(builder, VersionID(pair, "id1"), JUL_8_2010_1, "vsn1")
    add(builder, VersionID(pair, "id2"), JUL_8_2010_2, "vsn2")
    add(builder, VersionID(pair, "id3"), JUL_9_2010_1, "vsn3")
    add(builder, VersionID(pair, "id4"), AUG_2_2010_1, "vsn4")

    assertEquals(
      HashSet(
        // TODO [#2] Should the youngest or lastUpDated date be taken for the comparison?
        AggregateDigest(Seq("2010-07"), JUL_8_2010_1, DigestUtils.md5Hex("vsn1" + "vsn2" + "vsn3")),
        AggregateDigest(Seq("2010-08"), AUG_2_2010_1, DigestUtils.md5Hex("vsn4"))),
      HashSet(builder.digests: _*))
  }

  @Test
  def shouldBucketByYear {
    val builder = new DigestBuilder(withBucketing(YearlyCategoryFunction))

    add(builder, VersionID(pair, "id0"), JUN_6_2009_1, "vsn0")
    add(builder, VersionID(pair, "id1"), JUL_8_2010_1, "vsn1")
    add(builder, VersionID(pair, "id2"), JUL_8_2010_2, "vsn2")
    add(builder, VersionID(pair, "id3"), JUL_9_2010_1, "vsn3")
    add(builder, VersionID(pair, "id4"), AUG_2_2010_1, "vsn4")
    add(builder, VersionID(pair, "id5"), JAN_2_2011_1, "vsn5")
    add(builder, VersionID(pair, "id6"), AUG_11_2011_1, "vsn6")

    assertEquals(
      HashSet(
        // TODO [#2] Should the youngest or lastUpDated date be taken for the comparison?
        AggregateDigest(Seq("2009"), JUN_6_2009_1, DigestUtils.md5Hex("vsn0")),
        AggregateDigest(Seq("2010"), JUL_8_2010_1, DigestUtils.md5Hex("vsn1" + "vsn2" + "vsn3" + "vsn4")),
        AggregateDigest(Seq("2011"), JAN_2_2011_1, DigestUtils.md5Hex("vsn5" + "vsn6"))),
      HashSet(builder.digests: _*))
  }

  /**
   * The idea behind this is that a bucket should be immutable after it
   * has been digested, hence adding a new item to the same bucket
   * after it has been digested should result in an error.  
   */
  @Test
  def sealedBuckets = {
    val builder = new DigestBuilder(withBucketing(YearlyCategoryFunction))
    add(builder, VersionID(pair, "id0"), JUN_6_2009_1, "vsn0")
    builder.digests.foreach(_.digest)
    try {
      add(builder, VersionID(pair, "id1"), JUN_6_2009_1, "vsn1")
      fail("Expected to provoke SealedBucketException")
    }
    catch {
      case e:SealedBucketException =>
    }
  }
}