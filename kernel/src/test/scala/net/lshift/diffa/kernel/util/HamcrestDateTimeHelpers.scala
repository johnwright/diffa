package net.lshift.diffa.kernel.util

import org.joda.time.DateTime
import org.hamcrest.{Description, BaseMatcher}

/**
 * Helpers for use in JUnit Hamcrest (eg, assertThat) expressions allowing datetimes to be asserted upon in a nice
 * manner.
 */
object HamcrestDateTimeHelpers {
  def after(t:DateTime) = new BaseMatcher[DateTime] {
    def describeTo(desc: Description) { desc.appendText("after " + t) }
    def matches(obj: AnyRef) = obj.isInstanceOf[DateTime] && obj.asInstanceOf[DateTime].isAfter(t)
  }

  def before(t:DateTime) = new BaseMatcher[DateTime] {
    def describeTo(desc: Description) { desc.appendText("before " + t) }
    def matches(obj: AnyRef) = obj.isInstanceOf[DateTime] && obj.asInstanceOf[DateTime].isBefore(t)
  }
}