package net.lshift.diffa.kernel.differencing

import se.scalablesolutions.akka.actor.ActorRegistry
import se.scalablesolutions.akka.actor.Actor._
import org.slf4j.LoggerFactory


class CorrelationActorSupervisor(store:VersionCorrelationStore) {

  private val log = LoggerFactory.getLogger(getClass)

  def startActor(key:String) = {
    val actors = ActorRegistry.actorsFor(key)
    actors.length match {
      case 0 => {
        ActorRegistry.register(actorOf(new CorrelationActor(key, store)).start)
        log.info("Started actor for key: " + key)
      }
      case 1    => log.warn("Attempting to re-spawn actor for key: " + key)
      case x    => log.error("Too many actors for key: " + key + "; actors = " + x)
    }
  }

  def stopActor(key:String) = {
    val actors = ActorRegistry.actorsFor(key)
    actors.length match {
      case 1 => {
        ActorRegistry.unregister(actors(0))
        log.info("Stopped actor for key: " + key)
      }
      case 0    => log.warn("Could not resolve actor for key: " + key)
      case x    => log.error("Too many actors for key: " + key + "; actors = " + x)
    }
  }
}