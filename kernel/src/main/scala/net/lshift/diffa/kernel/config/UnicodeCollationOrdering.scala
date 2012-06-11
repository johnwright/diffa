package net.lshift.diffa.kernel.config

import java.util.{Locale, Comparator}
import com.ibm.icu.text.Collator


/**
 * Created with IntelliJ IDEA.
 * User: ceri
 * Date: 12/06/11
 * Time: 14:53
 * To change this template use File | Settings | File Templates.
 */

object UnicodeCollationOrdering extends Comparator[AnyRef] {
  lazy val neutralLocale = Locale.ROOT
  lazy val collator = Collator.getInstance(neutralLocale)

  def compare(a: AnyRef, b: AnyRef) = collator.compare(a, b)
}


object AsciiCollationOrdering extends Comparator[AnyRef] {
  def compare(a: AnyRef, b:AnyRef) = compare(a.asInstanceOf[String], b.asInstanceOf[String])

  def compare(a:String, b:String) = a.compareTo(b)
}