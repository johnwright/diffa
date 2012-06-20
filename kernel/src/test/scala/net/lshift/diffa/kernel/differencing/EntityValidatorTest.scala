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
package net.lshift.diffa.kernel.differencing

import org.junit.Test
import org.junit.experimental.theories.Theories
import org.junit.runner.RunWith
import java.io.IOException
import org.hamcrest.CoreMatchers.instanceOf
import org.junit.Assert._
import org.hamcrest.Matchers._
import org.junit.matchers.JUnitMatchers.containsString
import net.lshift.diffa.participant.scanning.ScanResultEntry
import org.joda.time.DateTime
import scala.collection.JavaConversions._
import org.hamcrest.Matcher
import net.lshift.diffa.participant.common.InvalidEntityException

object EntityValidatorTest {
  final val INVALID_ID: String = "\u26093"
}

// @RunWith(classOf[Theories])
class EntityValidatorTest {
  import EntityValidatorTest._

  @Test def shouldRaiseExceptionWithNonAsciiId {
    assertThatSome(exceptionForInvalidId, instanceOf(classOf[InvalidEntityException]))
  }

  def assertThatSome[T](exception: Option[T], matcher: Matcher[T]) =
    exception.map(assertThat(_, matcher)).getOrElse(fail("Recieved None when expecting Some(value)"))

  @Test def exceptionMessageShouldContainInvalidStringForId {
    val ex =
    assertThatSome(exceptionForInvalidId.map(_.getMessage), containsString(INVALID_ID))
  }

  @Test def shouldBeValidWithAlphaNumericStringForId {
    val validId = "foo4_-,."
    val entity = scanResultFor(id = validId)
    assertEquals(None, exceptionOf(validator.process(entity)))
  }

  @Test def shouldBeValidWithNullId {
    val entity = scanResultFor(id = null)
    assertEquals(exceptionOf(validator.process(entity)), None)
  }

  lazy val validator = EntityValidator

  lazy val entityWithInvalidAttributes = {
    scanResultFor(attributes = Map("property" -> INVALID_ID))
  }

  @Test
  def shouldBeInvalidWithNonPrintablesInAttributeValues {
    assertThatSome(exceptionOf(validator.process(entityWithInvalidAttributes)),
      is(instanceOf(classOf[InvalidEntityException])))
  }

  @Test
  def shouldReportNonPrintablesInAttributeValuesInException {
    assertThatSome(exceptionOf(validator.process(entityWithInvalidAttributes)).map(_.getMessage),
      containsString(INVALID_ID))
  }

  private def exceptionOf(thunk: => Unit): Option[Throwable] =
    try { thunk; None } catch { case e => Some(e) }

  private def scanResultFor(id: String = "id", attributes: Map[String, String] = Map()) = {
   new ScanResultEntry(id, null, null, attributes)
  }

  val  exceptionForInvalidId = exceptionOf(validator.process(scanResultFor(id = INVALID_ID)))

}

