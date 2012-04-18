/**
 * Copyright (C) 2012 LShift Ltd.
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
import java.util.ArrayList
import scala.collection.JavaConversions._
import net.lshift.diffa.participant.scanning.{DateAggregation, SetConstraint, ScanRequest}

class ScanRequestWriterTest {
  @Test
  def shouldGenerateEmptyStringForNoRequests() {
    assertEquals(
      "",
      ScanRequestWriter.writeScanRequests(Seq())
    )
  }

  @Test
  def shouldGenerateSimpleScanRequestForSingleBasicRequest() {
    assertEquals(
      "scan",
      ScanRequestWriter.writeScanRequests(Seq(new ScanRequest(new ArrayList(), new ArrayList())))
    )
  }

  @Test
  def shouldSerialiseConstraintsAndAggregations() {
    assertEquals(
      "scan?someString=ss&someString=tt&someDate-granularity=yearly",
      ScanRequestWriter.writeScanRequests(
        Seq(
          new ScanRequest(
            Seq(new SetConstraint("someString", Set("ss", "tt"))),
            Seq(new DateAggregation("someDate", "yearly")))
        ))
    )
  }

  @Test
  def shouldSerialiseMultipleRequests() {
    assertEquals(
      "scan?someString=ss&someDate-granularity=yearly\n" +
      "scan?someString=tt",
      ScanRequestWriter.writeScanRequests(
        Seq(
          new ScanRequest(
            Seq(new SetConstraint("someString", Set("ss"))),
            Seq(new DateAggregation("someDate", "yearly"))),
          new ScanRequest(
            Seq(new SetConstraint("someString", Set("tt"))),
            Seq())
        ))
    )
  }
}