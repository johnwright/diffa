package net.lshift.diffa.kernel.diag.cassandra

import org.junit.Assert._
import org.junit.{BeforeClass, AfterClass, Test}
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.hector.testutils.EmbeddedServerHelper
import net.lshift.diffa.kernel.config.DiffaPairRef
import org.joda.time.DateTime

object CassandraExplainLogStoreTest {

  val helper = new EmbeddedServerHelper("/cassandra.yaml")
  var schema: Schema = _

  @BeforeClass
  def setup {
    helper.setup()
    val cluster = HFactory.getOrCreateCluster("test-cluster", "localhost:9171")
    schema = new Schema(cluster = cluster, keyspaceName = "diffa_test")
    schema.init()
  }

  @AfterClass
  def stop {
    EmbeddedServerHelper.teardown()
    EmbeddedServerHelper.cleanup()
  }
}

class CassandraExplainLogStoreTest {

  val pair1 = DiffaPairRef(domain = "diffa", key = "pair")
  val pair2 = DiffaPairRef(domain = "other", key = "pair")

  @Test
  def insertsAndRetrievesExplainLogEntries {
    val store = new CassandraExplainLogStore(CassandraExplainLogStoreTest.schema)
    val pageSize = 100 // N.B. testing using a number higher than the expected number of results in each case
    val scanId = Some(123L)

    val source1 = "Somewhere"
    val msg1 = "Hello World"
    val beforeFirstInsert = new DateTime()
    store.logPairExplanation(scanId, pair1, source1, msg1)
    val afterFirstInsert = new DateTime()

    val soleEntry = store.retrieveExplanations(pair1, beforeFirstInsert, afterFirstInsert, pageSize)
    assertEquals(1, soleEntry.size)
    val entry1 = soleEntry.head
    assertEquals(msg1, entry1.msg)
    assertEquals(source1, entry1.source)
    assertEquals(scanId, entry1.scanId)
    assertEquals(None, entry1.attachmentId)

    Thread.sleep(5)// introduce a short delay between inserts

    val source2 = "Somewhere Else"
    val msg2 = "Goodbye World"
    val beforeSecondInsert = new DateTime()
    store.logPairExplanation(None, pair1, source2, msg2)
    val afterSecondInsert = new DateTime()

    val firstEntryOnly = store.retrieveExplanations(pair1, beforeFirstInsert, afterFirstInsert, pageSize)
    assertEquals(1, firstEntryOnly.size)
    assertEquals(entry1, firstEntryOnly.head)

    val secondEntryOnly = store.retrieveExplanations(pair1, beforeSecondInsert, afterSecondInsert, pageSize)
    assertEquals(1, secondEntryOnly.size)
    val entry2 = secondEntryOnly.head
    assertEquals(msg2, entry2.msg)
    assertEquals(source2, entry2.source)
    assertEquals(None, entry2.scanId)
    assertEquals(None, entry2.attachmentId)

    val bothEntries = store.retrieveExplanations(pair1, beforeFirstInsert, afterSecondInsert, pageSize)
    assertEquals(2, bothEntries.size)
    assertTrue("first entry must be present in the results", bothEntries.find(_ == entry1).isDefined)
    assertTrue("second entry must be prsent in the results", bothEntries.find(_ == entry2).isDefined)

    val source3 = "Another Place"
    val msg3 = "Hello?"
    store.logPairExplanation(None, pair2, source3, msg3)
    val entryForDifferentPair = store.retrieveExplanations(pair2, beforeFirstInsert, new DateTime(), pageSize)
    assertEquals(1, entryForDifferentPair.size)
  }

  @Test
  def insertsAndRetrievesExplainLogAttachments {
    val store = new CassandraExplainLogStore(CassandraExplainLogStoreTest.schema)
    val scanId = Some(123L)

    val timestamp = new DateTime()
    store.logPairExplanationAttachment(scanId, pair1, "Some Source", "Some Tag", timestamp, json => {
      json.writeStringField("foo", "bar")
      json.writeArrayFieldStart("baz")
      for (i <- 1 to 3) json.writeNumber(i)
      json.writeEndArray()
    })

    val correspondingEntry = store.retrieveExplanations(pair1, timestamp, new DateTime().plusMinutes(1), 1)
    assertEquals(1, correspondingEntry.size)
    val entry = correspondingEntry.head
    assertEquals("Some Source", entry.source)
    assertEquals("Attached object", entry.msg)
    assertEquals(scanId, entry.scanId)
    assertTrue("entry must have attachment ID", entry.attachmentId.isDefined)

    val maybeAttachment = store.retrieveAttachment(pair1, entry.attachmentId.get)
    assertTrue(maybeAttachment.isDefined)
    val attachment = maybeAttachment.get
    assertEquals(pair1.toString, attachment.get("pair").getTextValue)
    assertEquals(scanId.get, attachment.get("scanId").getLongValue)
    assertEquals("bar", attachment.get("foo").getTextValue)
    val array = attachment.get("baz")
    assertEquals(1, array.get(0).getIntValue)
    assertEquals(2, array.get(1).getIntValue)
    assertEquals(3, array.get(2).getIntValue)

    // TODO test without scan ID
  }

}