package net.lshift.diffa.messaging.json

import org.junit.Test
import org.junit.Assert._
import scala.collection.Map
import scala.collection.JavaConversions._
import net.lshift.diffa.kernel.frontend.wire._

class DefaultEventFormatMapperTest {

  val mapper = new DefaultEventFormatMapper

  @Test
  def mapsSingleWireEvent() {
    val event = WireEvent("baz", Map("foo" -> "bar"), List("a", "b", "c"))
    val serialized = JSONEncodingUtils.serializeEvent(event)
    val deserialized = mapper.map(serialized, "")
    assertEquals(Seq(event), deserialized)
  }

  @Test
  def mapsWireEventList() {
    val events = Seq(WireEvent("baz1", Map("foo1" -> "bar1"), List("a", "b", "c")),
                     WireEvent("baz2", Map("foo2" -> "bar2"), List("d", "e", "f")))
    val serialized = JSONEncodingUtils.serializeEventList(events)
    val deserialized = mapper.map(serialized, "")
    assertEquals(events, deserialized)
  }
}