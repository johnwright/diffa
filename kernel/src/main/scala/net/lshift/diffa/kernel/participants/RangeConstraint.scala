package net.lshift.diffa.kernel.participants

object RangeConstraint {

  /**
   * Starting point date constraint that accepts any date (ie, it has no bounds).
   */
  val anyDate = BaseQueryConstraint("date", DailyCategoryFunction(), Seq())
}