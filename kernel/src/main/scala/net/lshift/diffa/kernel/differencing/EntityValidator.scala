package net.lshift.diffa.kernel.differencing

import net.lshift.diffa.participant.scanning.ScanResultEntry
import org.joda.time.DateTime

/**
 * Created with IntelliJ IDEA.
 * User: ceri
 * Date: 12/06/18
 * Time: 18:41
 * To change this template use File | Settings | File Templates.
 */

case class ValidatableEntity(id:String, version:String, lastUpdated:DateTime, attributes: Map[String, String]) {

}

object EntityValidator {
  import scala.collection.JavaConversions._
  def maybe[T](x: T) = x match {
    case null => None
    case x => Some(x)
  }

  def validateCharactersIn(string: String) = {
    // println("Validate chars: " + string)
    if (!java.util.regex.Pattern.compile("^\\p{Graph}*$").matcher(string).matches())
      throw new InvalidEntityException(string)
  }
  def validate(e: ValidatableEntity): Unit = {
    // println("Validating: %s".format(this))
    if (e.id != null) validateCharactersIn(e.id)
    e.attributes.foreach { case (_, value) => validateCharactersIn(value) }
  }

  def validate(e: ScanResultEntry): Unit = validate(of(e))

  private def of(e: ScanResultEntry) = ValidatableEntity(e.getId, e.getVersion, e.getLastUpdated,
    maybe(e.getAttributes).map(_.toMap).getOrElse(Map()))


}