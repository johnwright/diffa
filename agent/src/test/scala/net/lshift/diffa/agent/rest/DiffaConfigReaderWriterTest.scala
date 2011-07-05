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
package net.lshift.diffa.agent.rest

import org.junit.Test
import org.junit.Assert._
import scala.collection.JavaConversions._
import net.lshift.diffa.kernel.frontend.DiffaConfig
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import net.lshift.diffa.agent.util.ConfigComparisonUtil
import net.lshift.diffa.kernel.config._

/*
* Test cases for the DiffaConfigReaderWriter.
*/
class DiffaConfigReaderWriterTest {
  @Test
  def roundtrip = {
    val config = new DiffaConfig(
      properties = Map("diffa.host" -> "localhost:1234", "a" -> "b"),
      users = Set(User("abc", "a@example.com")),
      endpoints = Set(
        Endpoint(name = "upstream1", contentType = "application/json",
          inboundUrl = "http://inbound", inboundContentType = "application/xml",
          scanUrl = "http://localhost:1234/scan", contentRetrievalUrl = "http://localhost:1234/content",
          categories = Map(
            "a" -> new RangeCategoryDescriptor("date", "2009", "2010"),
            "b" -> new SetCategoryDescriptor(Set("a", "b", "c")))),
        Endpoint(name = "downstream1", contentType = "application/json",
          scanUrl = "http://localhost:5432/scan", versionGenerationUrl = "http://localhost:5432/generate-version",
          categories = Map(
            "c" -> new PrefixCategoryDescriptor(1, 5, 1),
            "d" -> new PrefixCategoryDescriptor(1, 6, 1)
          ))),
      groups = Set(PairGroup("gaa"), PairGroup("gbb")),
      pairs = Set(
        PairDef("ab", "same", 5, "upstream1", "downstream1", "gaa", "0 0 0 * 0 0"),
        PairDef("ac", "same", 5, "upstream1", "downstream1", "gbb")),
      repairActions = Set(
        RepairAction(name="Resend Sauce", scope="entity", url="http://example.com/resend/{id}", pairKey="ab")
      )
    )

    val readerWriter = new DiffaConfigReaderWriter
    val baos = new ByteArrayOutputStream
    readerWriter.writeTo(config, null, null, null, null, null, baos)

    val expectedXml =
      <diffa-config>
        <property key="diffa.host">localhost:1234</property>
        <property key="a">b</property>
        <user name="abc" email="a@example.com"/>
        <endpoint name="upstream1" content-type="application/json"
                  inbound-url="http://inbound" inbound-content-type="application/xml"
                  scan-url="http://localhost:1234/scan" content-url="http://localhost:1234/content">
          <range-category name="a" data-type="date" lower="2009" upper="2010"/>
          <set-category name="b">
            <value>a</value>
            <value>b</value>
            <value>c</value>
          </set-category>
        </endpoint>
        <endpoint name="downstream1" content-type="application/json"
                  scan-url="http://localhost:5432/scan" version-url="http://localhost:5432/generate-version">
          <prefix-category name="c" prefix-length="1" max-length="5" step="1"/>
          <prefix-category name="d" prefix-length="1" max-length="6" step="1"/>
        </endpoint>
        <group name="gaa">
          <pair key="ab" upstream="upstream1" downstream="downstream1" version-policy="same" matching-timeout="5" scan-schedule="0 0 0 * 0 0">
            <repair-action url="http://example.com/resend/{id}" name="Resend Sauce" scope="entity" />
          </pair>
        </group>
        <group name="gbb">
          <pair key="ac" upstream="upstream1" downstream="downstream1" version-policy="same" matching-timeout="5"/>
        </group>
      </diffa-config>.toString

    ConfigComparisonUtil.assertConfigMatches(expectedXml, new String(baos.toByteArray, "UTF-8"))

    val newConfig = readerWriter.readFrom(null, null, null, null, null, new ByteArrayInputStream(baos.toByteArray))
    assertEquals(config, newConfig)
  }

}