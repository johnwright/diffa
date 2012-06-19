package net.lshift.diffa.kernel.differencing

/**
 * Created with IntelliJ IDEA.
 * User: ceri
 * Date: 12/06/18
 * Time: 17:52
 * To change this template use File | Settings | File Templates.
 */

class EntityValidationTest {

}

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

object EntityValidatorTest {
  final val INVALID_ID: String = "\u26093"
}

// @RunWith(classOf[Theories])
class EntityValidatorTest {
  import EntityValidatorTest._

  @Test
  def hasSameFieldsAsScanResultEntry {
    lazy val entry = ScanResultEntry.forEntity("theId", "aVersion", DateTime.now(), Map[String, String]())
    val validator = EntityValidator.of(entry)

    assertThat(validator.id, equalTo(entry.getId))
    assertThat(validator.version, equalTo(entry.getVersion))
    assertThat(validator.lastUpdated, equalTo(entry.getLastUpdated))
    assertThat(validator.attributes, equalTo(entry.getAttributes.toMap))

  }


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
    val validator = scanResultValidatorFor(id = validId)
    assertEquals(None, exceptionOf(validator.validate))
  }

  @Test def shouldBeValidWithNullId {
    val validator = scanResultValidatorFor(id = null)
    assertEquals(exceptionOf(validator.validate), None)
  }

  lazy val validatorWithInvalidAttributes: EntityValidator = {
    scanResultValidatorFor(attributes = Map("property" -> INVALID_ID))
  }

  @Test
  def shouldBeInvalidWithNonPrintablesInAttributeValues {
    assertThatSome(exceptionOf(validatorWithInvalidAttributes.validate),
      instanceOf(classOf[InvalidEntityException]))
  }

  @Test
  def shouldReportNonPrintablesInAttributeValuesInException {
    assertThatSome(exceptionOf(validatorWithInvalidAttributes.validate).map(_.getMessage),
      containsString(INVALID_ID))
  }



  private def exceptionOf(thunk: => Unit): Option[Throwable] =
    try { thunk; None } catch { case e => Some(e) }

  private def scanResultValidatorFor(id: String = "id", attributes: Map[String, String] = Map()) = {
    EntityValidator(id, null, null, attributes)
  }

  private def exceptionForInvalidId = exceptionOf(scanResultValidatorFor(id = INVALID_ID).validate)

}

