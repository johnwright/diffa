package net.lshift.diffa.kernel.frontend.wire

import reflect.BeanProperty
import java.util.Map
import java.util.List

/**
 * On-the-wire representation of an aggregate request.
 */
case class WireAggregateRequest(
  @BeanProperty var buckets:Map[String, String],
  @BeanProperty var constraints:List[WireConstraint]
) {
  def this() = this(null, null)
}