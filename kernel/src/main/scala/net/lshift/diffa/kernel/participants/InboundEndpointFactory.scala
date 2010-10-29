package net.lshift.diffa.kernel.participants

import net.lshift.diffa.kernel.config.Endpoint

/**
 * Trait implemented by factories that can generate inbound endpoint change receivers.
 */
trait InboundEndpointFactory {
  /**
   * Allows the agent to determine if this factory can handle inbound endpoints with the given url and content. If
   * the factory responds with true, then a subsequent request to ensure an endpoint is available will be sent.
   */
  def canHandleInboundEndpoint(url:String, contentType:String):Boolean

  /**
   * Indicates that the factory should ensure an receiving endpoint is available for the given endpoint. This call will
   * only be made if a previous call to <code>canHandleInboundEndpoint</code> has been made and returned a positive
   * result.
   */
  def ensureEndpointReceiver(e:Endpoint)

  /**
   * Indicates to the factory that the endpoint with the given key has been removed from the system. Note that the
   * system will not filter these events based on factory support, so factories should expect to see more removal
   * events than ensure events, and should silently any unknown endpoints.
   */
  def endpointGone(key:String)
}