package net.lshift.diffa.kernel.participants

import org.joda.time.{Interval, DateTime}

case class DateConstraint(start:DateTime, end:DateTime, f:CategoryFunction)
  extends BaseQueryConstraint("date", f, Seq(start.toString(), end.toString()))

case class SimpleDateConstraint(override val start:DateTime, override val end:DateTime) extends DateConstraint(start, end, DailyCategoryFunction()) {


  @Deprecated
  private val interval = if (start != null && end != null) {
    new Interval(start, end)
  } else {
    null
  }
  
  @Deprecated
  def contains(d: DateTime): Boolean = {
    if (interval == null) {
      if (start == null && end == null) {
        true
      } else if (start == null) {
        !end.isBefore(d)
      } else {
        !start.isAfter(d)
      }
    } else {
      interval.contains(d)
    }
  }
}