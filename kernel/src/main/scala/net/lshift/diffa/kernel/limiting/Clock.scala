package net.lshift.diffa.kernel.limiting

trait Clock {
  def currentTimeMillis: Long
}

object SystemClock extends Clock {
  def currentTimeMillis = System.currentTimeMillis
}