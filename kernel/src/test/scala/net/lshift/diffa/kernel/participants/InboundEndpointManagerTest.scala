package net.lshift.diffa.kernel.participants

import org.junit.Test
import org.easymock.EasyMock._
import net.lshift.diffa.kernel.config.{Endpoint, ConfigStore}
import org.junit.Assert._

/**
 * Test cases for the InboundEndpointManager.
 */
class InboundEndpointManagerTest {
  val configStore = createMock(classOf[ConfigStore])
  val manager = new InboundEndpointManager(configStore)
  val jsonFactory = new InboundEndpointFactory {
    var lastEp:Endpoint = null

    def canHandleInboundEndpoint(url: String, contentType: String) = url.startsWith("amqp") && contentType == "application/json"
    def ensureEndpointReceiver(e: Endpoint) = lastEp = e
    def endpointGone(key: String) = null
  }

  @Test
  def shouldIgnoreEndpointWhereNoInboundUrlIsConfigured {
    manager.onEndpointAvailable(Endpoint("e", "http://localhost/1234", "application/json", null, true))
  }

  @Test
  def shouldHandleEndpointWhereInboundUrlIsNotSupported {
    manager.onEndpointAvailable(Endpoint("e", "http://localhost/1234", "application/json", "amqp:queue.name", true))
  }

  @Test
  def shouldInformFactoryWhenValidEndpointIsAvailable {
    manager.registerFactory(jsonFactory)
    manager.onEndpointAvailable(Endpoint("e", "http://localhost/1234", "application/json", "amqp:queue.name", true))

    assertNotNull(jsonFactory.lastEp)
    assertEquals("e", jsonFactory.lastEp.name)
  }

  @Test
  def shouldNotInformFactoryWhenEndpointIsNotAcceptable {
    manager.registerFactory(jsonFactory)
    manager.onEndpointAvailable(Endpoint("e", "http://localhost/1234", "application/xml", "amqp:queue.name", true))

    assertNull(jsonFactory.lastEp)
  }

  @Test
  def shouldActivateStoredEndpoint {
    manager.registerFactory(jsonFactory)

    expect(configStore.listEndpoints).andReturn(Seq(Endpoint("e", "http://localhost/1234", "application/json", "amqp:queue.name", true)))
    replay(configStore)

    manager.onAgentAssemblyCompleted
    assertNotNull(jsonFactory.lastEp)
    assertEquals("e", jsonFactory.lastEp.name)
  }
}