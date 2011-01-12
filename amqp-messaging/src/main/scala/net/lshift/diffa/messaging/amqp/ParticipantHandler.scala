package net.lshift.diffa.messaging.amqp

import net.lshift.diffa.kernel.participants.Participant
import net.lshift.diffa.messaging.json.JSONEncodingUtils

// TODO lots of commonality between this and its counterpart in json-messaging
abstract class ParticipantHandler(participant: Participant) extends RpcRequestHandler {

  val endpoints: Map[String, String]

  /*
  val commonEndpoints = Map(
    "query_aggregate_digests" -> makeEndpoint(participant.queryAggregateDigests(_))
  )

  def makeEndpoint[A <: Any : Manifest, B <: Any : Manifest](f: A => B) = {
    val typeOfA = manifest[A]
    val typeOfB = manifest[B]
  }
*/
  def handleRequest(methodName: String, argument: String): String = {
    val endpoint = endpoints.getOrElse(methodName, throw new MissingEndpointException(methodName))
    // TODO handle missing endpoint by sending an error response
    //endpoint.get.apply(argument)
    error("TODO")
  }
  /*
      "query_aggregate_digests" -> skeleton(wire => serializeDigests(pack(participant.queryAggregateDigests(unpack(wire))))),
    "query_entity_versions" -> skeleton(wire => serializeDigests(pack(participant.queryEntityVersions(unpack(wire))))),
    "invoke" -> defineRpc((s:String) => s)(r => {
      val request = deserializeActionRequest(r)
      serializeActionResult(participant.invoke(request.actionId, request.entityId))
    }),
    "retrieve_content" -> defineRpc((s:String) => s)(req => {
      serializeEntityContent(participant.retrieveContent(deserializeEntityContentRequest(req)))
    })
   */

 /*
  def queryAggregateDigests(constraints:Seq[QueryConstraint]) : Seq[AggregateDigest]

  def queryEntityVersions(constraints:Seq[QueryConstraint]) : Seq[EntityVersion]

  def invoke(actionId:String, entityId:String) : InvocationResult

  def retrieveContent(identifier:String): String
  */
}