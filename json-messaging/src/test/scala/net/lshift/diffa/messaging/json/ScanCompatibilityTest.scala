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
package net.lshift.diffa.messaging.json

import net.lshift.diffa.participant.content.{ContentParticipantDelegator, ContentParticipantHandler}
import org.eclipse.jetty.server.handler.AbstractHandler
import org.eclipse.jetty.server.{Request, Server}
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}
import net.lshift.diffa.participant.common.ServletHelper
import org.easymock.EasyMock._
import org.easymock.{EasyMock, IAnswer}
import org.junit.{Before, Test}
import java.util.{Arrays, ArrayList}
import scala.collection.JavaConversions._
import org.joda.time.{DateTimeZone, DateTime, LocalDate}
import net.lshift.diffa.kernel.participants._
import net.lshift.diffa.participant.scanning.{ConstraintsBuilder, AggregationBuilder, ScanningParticipantHandler, ScanConstraint, ScanAggregation}
import net.lshift.diffa.participant.scanning.{ScanningParticipantDelegator, ScanResultEntry, DateGranularityEnum, DateAggregation, ByNameAggregation}
import net.lshift.diffa.participant.scanning.{StringPrefixAggregation, IntegerAggregation}

/**
 * Test ensuring that internal query constraint and aggregation types are passed and parsed by Scala participants.
 */
class ScanCompatibilityTest {
  import ScanCompatibilityTest._

  @Before
  def startServer() {
    ensureServerStarted()
  }

  @Before
  def reset() {
    resetAll()
  }

  @Test
  def shouldBeAbleToPerformEmptyScan() {
    stubAggregationBuilder(req => new AggregationBuilder(req))
    stubConstraintBuilder(req => new ConstraintsBuilder(req))
    expectQuery(Seq(), Seq())
    replayAll()

    scanningRestClient.scan(Seq(), Map())
    verifyAll()
  }

  @Test
  def shouldBeAbleToPerformDateRangeConstrainedScan() {
    stubAggregationBuilder(req => new AggregationBuilder(req))
    stubConstraintBuilder(req => {
      val builder = new ConstraintsBuilder(req)
      builder.maybeAddDateRangeConstraint("bizDate")
      builder
    })
    expectQuery(Seq(new net.lshift.diffa.participant.scanning.DateRangeConstraint("bizDate", new LocalDate(2011, 7, 1), new LocalDate(2011, 7, 31))), Seq())
    replayAll()

    scanningRestClient.scan(Seq(DateRangeConstraint("bizDate", new LocalDate(2011, 7, 1), new LocalDate(2011, 7, 31))), Map())
    verifyAll()
  }

  @Test
  def shouldBeAbleToPerformTimeRangeConstrainedScan() {
    stubAggregationBuilder(req => new AggregationBuilder(req))
    stubConstraintBuilder(req => {
      val builder = new ConstraintsBuilder(req)
      builder.maybeAddTimeRangeConstraint("bizTime")
      builder
    })
    expectQuery(Seq(new net.lshift.diffa.participant.scanning.TimeRangeConstraint("bizTime", new DateTime(2011, 7, 1, 10, 36, 0, 0, DateTimeZone.UTC), new DateTime(2011, 7, 31, 11, 36, 0, 0, DateTimeZone.UTC))), Seq())
    replayAll()

    scanningRestClient.scan(Seq(DateTimeRangeConstraint("bizTime", new DateTime(2011, 7, 1, 10, 36, 0, 0, DateTimeZone.UTC), new DateTime(2011, 7, 31, 11, 36, 0, 0, DateTimeZone.UTC))), Map())
    verifyAll()
  }

  @Test
  def shouldBeAbleToPerformSetConstrainedScan() {
    stubAggregationBuilder(req => new AggregationBuilder(req))
    stubConstraintBuilder(req => {
      val builder = new ConstraintsBuilder(req)
      builder.maybeAddSetConstraint("someString")
      builder
    })
    expectQuery(Seq(new net.lshift.diffa.participant.scanning.SetConstraint("someString", Set("aa", "bb"))), Seq())
    replayAll()

    scanningRestClient.scan(Seq(SetQueryConstraint("someString", Set("aa", "bb"))), Map())
    verifyAll()
  }

  @Test
  def shouldBeAbleToPerformIntegerConstrainedScan() {
    stubAggregationBuilder(req => new AggregationBuilder(req))
    stubConstraintBuilder(req => {
      val builder = new ConstraintsBuilder(req)
      builder.maybeAddIntegerRangeConstraint("someInt")
      builder
    })
    expectQuery(Seq(new net.lshift.diffa.participant.scanning.IntegerRangeConstraint("someInt", 5, 20)), Seq())
    replayAll()

    scanningRestClient.scan(Seq(IntegerRangeConstraint("someInt", 5, 20)), Map())
    verifyAll()
  }

  @Test
  def shouldBeAbleToPerformStringPrefixConstrainedScan() {
    stubAggregationBuilder(req => new AggregationBuilder(req))
    stubConstraintBuilder(req => {
      val builder = new ConstraintsBuilder(req)
      builder.maybeAddStringPrefixConstraint("someString")
      builder
    })
    expectQuery(Seq(new net.lshift.diffa.participant.scanning.StringPrefixConstraint("someString", "bl")), Seq())
    replayAll()

    scanningRestClient.scan(Seq(PrefixQueryConstraint("someString", "bl")), Map())
    verifyAll()
  }

  @Test
  def shouldBeAbleToPerformDateAggregatedScan() {
    stubAggregationBuilder(req => {
      val builder = new AggregationBuilder(req)
      builder.maybeAddDateAggregation("bizDate")
      builder.maybeAddDateAggregation("bizDate2")
      builder.maybeAddDateAggregation("bizDate3")
      builder
    })
    stubConstraintBuilder(req => new ConstraintsBuilder(req))
    expectQuery(Seq(), Seq(
      new DateAggregation("bizDate", DateGranularityEnum.Yearly),
      new DateAggregation("bizDate2", DateGranularityEnum.Monthly),
      new DateAggregation("bizDate3", DateGranularityEnum.Daily)))
    replayAll()

    scanningRestClient.scan(Seq(), Map("bizDate" -> YearlyCategoryFunction, "bizDate2" -> MonthlyCategoryFunction, "bizDate3" -> DailyCategoryFunction))
    verifyAll()
  }

  @Test
  def shouldBeAbleToPerformByNameAggregatedScan() {
    stubAggregationBuilder(req => {
      val builder = new AggregationBuilder(req)
      builder.maybeAddByNameAggregation("someString")
      builder
    })
    stubConstraintBuilder(req => new ConstraintsBuilder(req))
    expectQuery(Seq(), Seq(new ByNameAggregation("someString")))
    replayAll()

    scanningRestClient.scan(Seq(), Map("someString" -> ByNameCategoryFunction))
    verifyAll()
  }

  @Test
  def shouldBeAbleToPerformIntegerAggregatedScan() {
    stubAggregationBuilder(req => {
      val builder = new AggregationBuilder(req)
      builder.maybeAddIntegerAggregation("someInt")
      builder
    })
    stubConstraintBuilder(req => new ConstraintsBuilder(req))
    expectQuery(Seq(), Seq(new IntegerAggregation("someInt", 100)))
    replayAll()

    scanningRestClient.scan(Seq(), Map("someInt" -> IntegerCategoryFunction.AutoNarrowingIntegerCategoryFunction(100, 10)))
    verifyAll()
  }

  @Test
  def shouldBeAbleToPerformPrefixAggregatedScan() {
    stubAggregationBuilder(req => {
      val builder = new AggregationBuilder(req)
      builder.maybeAddStringPrefixAggregation("someString")
      builder
    })
    stubConstraintBuilder(req => new ConstraintsBuilder(req))
    expectQuery(Seq(), Seq(new StringPrefixAggregation("someString", 2)))
    replayAll()

    scanningRestClient.scan(Seq(), Map("someString" -> StringPrefixCategoryFunction(2, 10, 2)))
    verifyAll()
  }
}

object ScanCompatibilityTest {
  val scanningParticipant = createStrictMock(classOf[ScanningParticipantHandler])
  val serverPort = 41255

  lazy val server = new ParticipantServer(serverPort, scanningParticipant)
  lazy val scanningRestClient = new ScanningParticipantRestClient("http://localhost:" + serverPort + "/scan")

  def stubAggregationBuilder(a:(HttpServletRequest) => AggregationBuilder) {
    expect(scanningParticipant.determineAggregations(anyObject.asInstanceOf[HttpServletRequest])).andStubAnswer(new IAnswer[java.util.List[ScanAggregation]] {
      def answer() = {
        val req = EasyMock.getCurrentArguments()(0).asInstanceOf[HttpServletRequest]
        a(req).toList
      }
    })
  }

  def stubConstraintBuilder(c:(HttpServletRequest) => ConstraintsBuilder) {
    expect(scanningParticipant.determineConstraints(anyObject.asInstanceOf[HttpServletRequest])).andStubAnswer(new IAnswer[java.util.List[ScanConstraint]] {
      def answer() = {
        val req = EasyMock.getCurrentArguments()(0).asInstanceOf[HttpServletRequest]
        c(req).toList
      }
    })
  }

  def expectQuery(constraints:Seq[ScanConstraint], aggregations:Seq[ScanAggregation]) {
    expect(scanningParticipant.doQuery(constraints, aggregations)).andReturn(new ArrayList[ScanResultEntry])
  }

  def ensureServerStarted() {
    if (!server.isRunning) server.start()
  }

  def replayAll() {
    replay(scanningParticipant)
  }

  def resetAll() {
    reset(scanningParticipant)
  }

  def verifyAll() {
    verify(scanningParticipant)
  }
}

class ParticipantServer(port:Int, scanning:ScanningParticipantHandler) {
  private val scanningAdapter = new ScanningParticipantDelegator(scanning)

  private val server = new Server(port)
  server.setHandler(new AbstractHandler {
    override def handle(target: String, jettyReq: Request, request: HttpServletRequest, response: HttpServletResponse): Unit = {
      if (target.startsWith("/scan")) {
        scanningAdapter.handleRequest(request, response)
      } else {
        response.setStatus(HttpServletResponse.SC_NOT_FOUND)
        ServletHelper.writeResponse(response, "Unknown path " + target)
      }

      jettyReq.setHandled(true)
    }
  })
  server.setStopAtShutdown(true)

  def start() {
    server.start()
  }

  def isRunning = server.isRunning
}