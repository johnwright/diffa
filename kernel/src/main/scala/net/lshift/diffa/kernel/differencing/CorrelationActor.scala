package net.lshift.diffa.kernel.differencing

import org.slf4j.{Logger, LoggerFactory}
import net.lshift.diffa.kernel.events.VersionID
import se.scalablesolutions.akka.actor.{ActorRegistry, Actor}
import org.joda.time.DateTime

class CorrelationActor(id:String, store:VersionCorrelationStore) extends Actor {

  val logger:Logger = LoggerFactory.getLogger(getClass)

  self.id_=(id)

  def receive = {
    case StoreUpstreamMessage(id, date, lastUpdated, vsn) => {
      logger.info("received upstream store")
      val c = store.storeUpstreamVersion(id, date, lastUpdated, vsn)
      self.reply(c)
    }
    case StoreDownstreamMessage(id, date, lastUpdated, uvsn, dvsn) => {
      logger.info("received downstream store")
      val c = store.storeDownstreamVersion(id, date, lastUpdated, uvsn, dvsn)
      self.reply(c)
    }
    case ClearUpstreamMessage(id) => {
      logger.info("received upstream clear")
      val c = store.clearUpstreamVersion(id)
      self.reply(c)
    }
    case ClearDownstreamMessage(id) => {
      logger.info("received downstream clear")
      val c = store.clearDownstreamVersion(id)
      self.reply(c)
    }
    case _ => logger.error("received unknown message")
  }
}

case class ActorMessage
case class StoreUpstreamMessage(id:VersionID, date:DateTime, lastUpdated:DateTime, vsn:String) extends ActorMessage
case class StoreDownstreamMessage(id:VersionID, date:DateTime, lastUpdated:DateTime, uvsn:String, dvsn:String) extends ActorMessage
case class ClearUpstreamMessage(id:VersionID) extends ActorMessage
case class ClearDownstreamMessage(id:VersionID) extends ActorMessage

object CorrelationActor {

  val log:Logger = LoggerFactory.getLogger(getClass)

  def storeUpstreamVersion(id:VersionID, date:DateTime, lastUpdated:DateTime, vsn:String) : Correlation =
    withActor(id, StoreUpstreamMessage(id, date, lastUpdated, vsn))
  def storeDownstreamVersion(id:VersionID, date:DateTime, lastUpdated:DateTime, uvsn:String, dvsn:String) : Correlation =
    withActor(id, StoreDownstreamMessage(id, date, lastUpdated, uvsn, dvsn))
  def clearUpstreamVersion(id:VersionID) : Correlation = withActor(id, ClearUpstreamMessage(id))
  def clearDownstreamVersion(id:VersionID) : Correlation = withActor(id, ClearDownstreamMessage(id))

  def withActor(id:VersionID, msg:ActorMessage) : Correlation = {
    val actors = ActorRegistry.actorsFor(id.pairKey)
    actors.length match {
      case 1 => {
        val result = actors(0) !! msg
        result match {
          case Some(c) => c.asInstanceOf[Correlation]
          case None => {
            log.error("Message timeout")
            throw new RuntimeException("Message timeout")
          }
        }
      }
      case 0    =>  {
        log.error("Could not resolve actor for key: " + id.pairKey)
        throw new RuntimeException("Unresolvable pair: " + id.pairKey)
      }
      case x    => {
        log.error("Too many actors for key: " + id.pairKey + "; actors = " + x)
        throw new RuntimeException("Too many actors: " + id.pairKey)
      }
    }
  }
}
